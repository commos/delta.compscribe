try {
    require("source-map-support").install();
} catch(err) {
}
require("./../target/js/goog/bootstrap/nodejs.js");
require("./../target/js/test.js");
goog.require("commos.run_tests");
goog.require("cljs.nodejscli");
