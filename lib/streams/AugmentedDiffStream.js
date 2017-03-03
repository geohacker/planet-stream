var K = require('kefir');
var request = require('request-promise');
var R = require('ramda');
var AugmentedDiffParser = require('../parsers/AugmentedDiffParser.js');
var moment = require('moment');
/* Function that takes an options object and returns
 * an augmented diff stream with an associated 'current state'
 */
function AugmentedDiffStream (opts) {
  opts = opts || {};

  var pollFreq = opts.pollFreq || 60 * 1000;
  var state_param = opts.state_param || '_status';
  var id_param = opts.id_param || '?id=';
  var log = opts.log;

  var state_url = opts.overpass_url + '/api/augmented_diff' || 'http://overpass-api.de/api/augmented_diff';
  var base_url = opts.overpass_url  + '/api/interpreter' || 'http://overpass-api.de/api/interpreter';

  // State property
  var state = K.fromPoll(pollFreq, function () {
    log.info('Polling Overpass');
    return K.fromPromise(request(state_url + state_param));
  })
        .flatMap()
        .map(function (x) { return Number(x); })
        .skipDuplicates()
        .toProperty(R.always(0));

  // Stream of URls
  var urlStrings = state
        .changes()
        .map(function (x) {
          var thisMinute = (x * 60 + 1347432900) * 1000;
          var nextMinute = (x * 60 + 1347432900 + 60) * 1000;
          var since = moment.utc(thisMinute).format();
          var until = moment.utc(nextMinute).format();
          // var since = new Date(thisMinute).toISOString();
          // var until = new Date(nextMinute).toISOString()
          var qs = '[timeout:3600][adiff:"' + since + '","' + until + '"];(node(changed:"' + since + '","' + until + '");way(changed:"' + since + '","' + until + '");rel(changed:"' + since + '","' + until + '"););out meta geom;'
          // var qs = '[adiff:"'$SINCE'","'$UNTIL'"];(node(changed:"'$SINCE'","'$UNTIL'");way(changed:"'$SINCE'","'$UNTIL'");rel(changed:"'$SINCE'","'$UNTIL'"););out meta geom;'
          return base_url + '?data=' + qs;
        })
        .map(function (x) {
          log.info('Retrieving ' + x + ' from Overpass');
          return x;
        });

  var parsedData = urlStrings
        .flatMap(function (x) {
          return K.fromPromise(request(x).catch(function (err) {log.debug(err)}));
        })
        .map(function (x) {
          log.info('Data length: ' + x.length);
          return x;
        })
        .flatMapConcat(AugmentedDiffParser)
        .map(R.toString);

  return {state: state, stream: parsedData};
}

module.exports = AugmentedDiffStream;
