
let _ = require('lodash')
let co = require('co')
let requireAll = require('require-all')

/* global VERSION */

let commands = requireAll({
  dirname: `${__dirname}/commands`
})

let HELPTEXT = `

  Stinkerpg ${VERSION}
  ==============================

  A Postgresql command line tool.

  Commands:
    stinkerpg sync            Synchronize differences between two databases.
    stinkerpg -h | --help     Show this screen.

`

 // Some notes --> process.stdout.write(" RECORDS INSERTED: Total = #{records_processed} | Per Second = #{rps} | Percent Complete = %#{pc}          \r");

module.exports = function (argv) {
  return co(function *() {
    let command = _.first(argv['_'])
    argv['_'] = argv['_'].slice(1)
    if (commands[command]) {
      yield commands[command](argv)
    } else {
      console.log(HELPTEXT)
    }

    process.exit()
  })
  .catch(function (err) {
    console.log('ERROR')
    console.log(err)
  })
}
