"use strict";

const config = require("./database"),
	{Collection} = require("../dist/lib"),
	{assert} = require("chai");

describe("Table Collection", function() {
	const table = new Collection("FirstTable", {
			Id: {
				type: "Id"
			}
		}, config),
		table2 = new Collection("SecondTable", {
			Id: {type: "Id"},
			name: {
				type: "String",
				length: 60,
				required: true
			},
			slug: {
				type: "String",
				length: 160,
				index: true,
				validate(value, columns) {
					return columns.name.toLowerCase();
				}
			},
			description: {
				type: "String"
			},
			age: {
				type: "Int"
			},
			grade: {
				type: "Float"
			},
			isAlive: {
				type: "Boolean"
			},
			settings: {
				type: "Object"
			},
			hobbies: {
				type: "Array"
			},
			tester: {
				type: "ForeignId",
				unsigned: true,
				foreign: {
					key: 'tester_foreign',
					name: table.getName(),
					column: "Id",
					onDelete: "cascade",
					onUpdate: "cascade"
				}
			}
		}, config);

	const newSchema = {
		Id: {type: "Id"},
		name: {
			type: "String",
			length: 60,
			required: true
		},
		slug: {
			type: "String",
			length: 160,
			validate(value, columns) {
				return columns.name.toLowerCase();
			}
		},
		description: {
			type: "String"
		},
		age: {
			type: "Int",
			index: true
		},
		grade: {
			type: "Float",
			length: 2
		},
		skills: {
			type: "Enum",
			enum: ["Cooking", "Eating"],
			defaultValue: "Eating"
		},
		tester: {
			type: "ForeignId",
			unsigned: true,
			foreign: {
				key: 'newTester_foreign',
				name: table.getName(),
				column: "Id",
				onDelete: "cascade",
				onUpdate: "cascade"
			}
		}
	};

	const table3 = new Collection("TableThree", newSchema, config);

	test("Create", async () => {
		const [err] = await table.create({engine: "InnoDB"});

		assert.isNull(err);

		const [err2] = await table2.create({engine: "InnoDB"});

		assert.isNull(err2);
	});

	// Alter first table
	test("Alter", async () => {
		const [err] = await table2.alter(table2.getSchema(), newSchema);

		assert.isNull(err);

		table2.schema = newSchema;
	});

	test("Clone", async () => {
		// Add data 
		const [err] = await table2.insert({
			name: "Samanta"
		});

		assert.isNull(err);

		const [err2] = await table2.clone("TableThree");

		assert.isNull(err2);

		const [err3, results] = await table3.find();

		assert.isNull(err3);
		console.log(results);
	});

	test("Drop", async () => {
		// Remove the child table first to avoid error
		const [err] = await table2.drop();

		assert.isNull(err);

		const [err2] = await table.drop();

		assert.isNull(err2);

		const [err3] = await table3.drop();

		assert.isNull(err3);
	});
});