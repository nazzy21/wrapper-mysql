import * as _ from "underscore";

_.extend(_, {
	serialize,
	unserialize,
	devAssert	
});

module.exports = _;

function serialize(value) {
    try {
        return JSON.stringify(value);
    } catch(e) {
        return value;
    }
}

function unserialize(value) {
    try {
        return JSON.parse(value);
    } catch(e) {
        return value;
    }
}

function devAssert(condition, message) {
    if (_.isBoolean(condition) && !condition) {
        throw new Error(message);
    }
}
