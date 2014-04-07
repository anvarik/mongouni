var mongo = require('mongodb');

var Server = mongo.Server,
    Db = mongo.Db,
    BSON = mongo.BSONPure;

var nameOfDB = exports.nameOfDB = 'weather';
var nameOfCollection = exports.nameOfCollection = 'data';


var server = new Server('localhost', 27017, {
    auto_reconnect: true
});
db = new Db(nameOfDB, server);

db.open(function (err, db) {
    if (!err) {
        console.log("Connecting to " + nameOfDB + " database");
        db.collection(nameOfCollection, {
            strict: true
        }, function (err, collection) {
            if (err) {
                console.log('Collection does not exist');
                throw (err);
            } else {
                console.log("Connected to " + nameOfCollection + " database");
                var cursor = collection.find({}).sort([["State", 1], ["Temperature", -1]]);
                var state = "";
                cursor.each(function (err, doc) {
                    if (err)
                        throw (err);
                    else {
                        if (doc !== null) {
                            var currentState = doc.State;
                            if (state !== currentState) {
                                state = currentState;
                                doc['month_high'] = true;
                                var qry = {
                                    _id : doc['_id']
                                };
                                collection.update(qry, doc, function (err, updated) {
                                    if (err)
                                        throw err;
                                });
                            }
                        } else {
                            db.close();
                        }
                    };
                });


            }
        });
    }
});
