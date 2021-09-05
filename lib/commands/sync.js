
let _ = require('lodash');
let Promise = require('bluebird');

let inquirer = require('inquirer');
let co = require('co');
let colors = require('colors');

let moment = require('moment');
let { Client } = require('pg');
const Cursor = require('pg-cursor');
let maxId = '~~~~~~~~~~~~~~~~~~~~~~';

let HELPTEXT = `

  Stinkerpg Sync
  ==============================

  Sync two RethinkDB databases.

  Usage:
    stinkerpg sync [options]
    stinkerpg sync --sh host[:port] --th host[:port] --sd dbName --td dbName
    stinkerpg sync -h | --help

  Options:
    --sh, --sourceHost=<host[:port]>    Source host, defaults to 'localhost:21015'
    --th, --targetHost=<host[:port]>    Target host, defaults to 'localhost:21015'
    --sd, --sourceDB=<dbName>           Source database
    --td, --targetDB=<dbName>           Target database

    --pt, --pickTables=<table1,table2>  Comma separated list of tables to sync (whitelist)
    --ot, --omitTables=<table1,table2>  Comma separated list of tables to ignore (blacklist)
                                        Note: '--pt' and '--ot' are mutually exclusive options.

    --user                              Source and Target username
    --password                          Source and Target password

    --su                                Source username, overrides --user
    --sp                                Source password, overrides --password

    --tu                                Target username, overrides --user
    --tp                                Target password, overrides --password

`

module.exports = function* (argv) {
  let startTime
  let sHost = argv.sh ? argv.sh : argv.sourceHost ? argv.sourceHost : 'localhost:5432'
  let tHost = argv.th ? argv.th : argv.targetHost ? argv.targetHost : 'localhost:5432'
  let sourceHost = _.first(sHost.split(':'))
  let targetHost = _.first(tHost.split(':'))
  let sourcePort = parseInt(_.last(sHost.split(':')), 10) || 5432
  let targetPort = parseInt(_.last(tHost.split(':')), 10) || 5432
  let sourceDB = argv.sd ? argv.sd : argv.sourceDB ? argv.sourceDB : 'pci_eod'
  let targetDB = argv.td ? argv.td : argv.targetDB ? argv.targetDB : 'pci_eod'
  let pickTables = argv.pt ? argv.pt : argv.pickTables ? argv.pickTables : null
  let omitTables = argv.ot ? argv.ot : argv.omitTables ? argv.omitTables : null
  let sourceUser = argv.su ? argv.su : argv.user ? argv.user : 'pcidbreadonly'
  let sourcePassword = argv.sp ? argv.sp : argv.password ? argv.password : 'l3tm31n'
  let targetUser = argv.tu ? argv.tu : argv.user ? argv.user : 'postgres'
  let targetPassword = argv.tp ? argv.tp : argv.password ? argv.password : 'courtjesters'

  pickTables = _.isString(pickTables) ? pickTables.split(',') : null
  omitTables = _.isString(omitTables) ? omitTables.split(',') : null

  if (argv.h || argv.help) {
    console.log(HELPTEXT)
    return
  }

  if (pickTables && omitTables) {
    console.log('pickTables and omitTables are mutually exclusive options.')
    return
  }

  if (!sourceDB || !targetDB) {
    console.log('Source and target databases are required!')
    console.log(HELPTEXT)
    return
  }

  if (`${sourceHost}:${sourcePort}` === `${targetHost}:${targetPort}` && sourceDB === targetDB) {
    console.log('Source and target databases must be different if cloning on same server!')
    return
  }

  // Verify source database
  let clientSource = new Client({
    host: sourceHost,
    port: sourcePort,
    database: sourceDB,
    user: sourceUser,
    password: sourcePassword,
    // max: 10,
    // idleTimeoutMillis: 30000,
    // connectionTimeoutMillis: 2000,
  });
  yield clientSource.connect();

  // get sourceTableList
  let sourceTableList = yield clientSource.query(`
    SELECT "table_name" as "table" 
    FROM information_schema.tables
    WHERE "table_schema" = 'public' AND "table_type" = 'BASE TABLE'
    ORDER BY 1;
  `);
  sourceTableList = sourceTableList.rows.map(item => item.table);


  let clientTarget = new Client({
    host: targetHost,
    port: targetPort,
    database: targetDB,
    user: targetUser,
    password: targetPassword,
    // max: 10,
    // idleTimeoutMillis: 30000,
    // connectionTimeoutMillis: 2000,
  });

  yield clientTarget.connect();

  // get targetTableList
  let tagetTableList = yield clientTarget.query(`
    SELECT "table_name" as "table" 
    FROM information_schema.tables
    WHERE "table_schema" = 'public' AND "table_type" = 'BASE TABLE'
    ORDER BY 1;
  `)

  tagetTableList = tagetTableList.rows.map(item => item.table);

  if (!sourceTableList.every(ai => tagetTableList.includes(ai))) {
    console.log('Source DB does not exist!');
    return;
  }



  if (pickTables && !_.every(pickTables, (table) => sourceTableList.includes(table))) {
    console.log(colors.red('Not all the tables specified in --pickTables exist!'));
    return;
  }

  if (omitTables && !_.every(omitTables, (table) => sourceTableList.includes(table))) {
    console.log(colors.red('Not all the tables specified in --omitTables exist!'));
    return;
  }

  let confMessage = `
    ${colors.green('Ready to synchronize!')}
    The database '${colors.yellow(sourceDB)}' on '${colors.yellow(sourceHost)}:${colors.yellow(sourcePort)}' will be synchronized to the '${colors.yellow(targetDB)}' database on '${colors.yellow(targetHost)}:${colors.yellow(targetPort)}'
    This will modify records in the '${colors.yellow(targetDB)}' database on '${colors.yellow(targetHost)}:${colors.yellow(targetPort)}' if it exists!
  `;



  if (pickTables) {
    confMessage += `  ONLY the following tables will be synchronized: ${colors.yellow(pickTables.join(','))}\n`;
  }
  if (omitTables) {
    confMessage += `  The following tables will NOT be synchronized: ${colors.yellow(omitTables.join(','))}\n`;
  }

  console.log(confMessage);

  let answer = yield inquirer.prompt([{
    type: 'confirm',
    name: 'confirmed',
    message: 'Proceed?',
    default: false
  }]);



  if (!answer.confirmed) {
    console.log(colors.red('ABORT!'));
    return;
  }

  startTime = moment();

  let tablesToSync;
  if (pickTables) {
    tablesToSync = pickTables;
  } else if (omitTables) {
    tablesToSync = _.difference(sourceTableList, omitTables);
  } else {
    tablesToSync = sourceTableList;
  }

  let sp = clientSource;
  let tp = clientTarget;


  for (let table of tablesToSync) {
    let total_records = yield sp.query(`
      SELECT COUNT(*) as "cnt" FROM public."${table}";
    `);
    total_records = Number(total_records.rows[0].cnt);
    let records_processed = 0;
    let last_records_processed = 0;
    let perf_stat = [];
    let status_interval = 500;
    let tableDone = false;
    let created = 0;
    let updated = 0;
    let deleted = 0;

    console.log(`Synchronizing ${total_records} records in ${table}...                                                                  `)
    // let sourceCursor = yield sr.db(sourceDB).table(table).orderBy({index: r.asc('id')})
    //   .map(function (row) { return {id: row('id'), hash: r.uuid(row.toJSON())} })
    //   .run({cursor: true});

    let sourceCursor = sp.query(new Cursor(`
      SELECT s."id",
      md5(CAST((s.*)AS text)) as "hash", 
      row_to_json(s.*) as "json" 
      FROM public."${table}" as s 
      ORDER BY s."id";
    `));


    // let targetCursor = yield tr.db(targetDB).table(table).orderBy({index: r.asc('id')})
    //   .map(function (row) { return {id: row('id'), hash: r.uuid(row.toJSON())} })
    //   .run({cursor: true})

    let targetCursor = sp.query(new Cursor(`
      SELECT t."id", 
      md5(CAST((t.*)AS text)) as "hash"
      FROM public."${table}" as t 
      ORDER BY t."id";
    `));

    let si = {};
    let ti = {};

    si = yield getNextIdx(sourceCursor, si)
    ti = yield getNextIdx(targetCursor, ti)

    co(function* () {
      while (!tableDone) {
        perf_stat.unshift(records_processed - last_records_processed)
        while (perf_stat.length > 120) {
          perf_stat.pop()
        }
        let rps = (_.reduce(perf_stat, (a, b) => a + b) / (perf_stat.length * (status_interval / 1000))).toFixed(1)
        let pc = ((records_processed / total_records) * 100).toFixed(1)
        process.stdout.write(` RECORDS SYNCHRONIZED: ${records_processed} | ${rps} sec. | %${pc} | created ${created} | updated ${updated} | deleted ${deleted}          \r`)
        last_records_processed = records_processed;

        yield Promise.delay(status_interval);
      }
    })

    while (si.id !== maxId || ti.id !== maxId) {
      if (si.id === ti.id) {
        if (si.hash !== ti.hash) {

          // let record = yield sr.db(sourceDB).table(table).get(si.id).run()
          let record = si.json;
          console.log('update', record);

          // yield tr.db(targetDB).table(table).get(si.id).replace(record).run()
          yield tp.query(`
            UPDATE public."${table}" 
              SET 
              ${Object.keys(record).map((key) => '"' + key + '" = ' + obj[key] + "'").join(" ")}
            WHERE "id" = '${si.id}';
          `);
          updated += 1;
        }
        si = yield getNextIdx(sourceCursor, si);
        ti = yield getNextIdx(targetCursor, ti);
        records_processed += 1;
      } else if (si.id < ti.id) {

        // let record = yield sr.db(sourceDB).table(table).get(si.id).run()
        let record = si.json;

        console.log('insert', record);

        yield tp.query(`
          INSERT INTO public."${table}" 
            (\'${Object.keys(record).join('\',\'')}\') 
          VALUES(\'${Object.values(record).join('\',\'')}\');
        `);
        si = yield getNextIdx(sourceCursor, si);
        records_processed += 1;
        created += 1;
      } else if (si.id > ti.id) {
        // yield tr.db(targetDB).table(table).get(ti.id).delete().run()
        yield tp.query(`
          DELETE FROM public."${table}" 
          WHERE "id" = '${ti.id}';
        `);
        ti = yield getNextIdx(targetCursor, ti);
        deleted += 1;
      }
    }

    tableDone = true
  }

  console.log(colors.green(`DONE! Completed in ${startTime.fromNow(true)}`));

  // clientSource.release();
  // clientTarget.release();

}

var getNextIdx = function (cursor, idx) {

  return new Promise((resolve) => {
    if (idx.id !== maxId) {
      cursor.read(1, (err, rows) => {
        if (!err || rows.length) {
          idx = rows[0];
          console.log({ idx });
        } else {
          idx = {
            hash: '',
            id: maxId
          };
          cursor.close();
        }
        
        resolve(idx);
      });

    } else {
      resolve(idx);
    }

  });

}
