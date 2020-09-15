"use strict";

const {assert} = require("chai");

const config = require("./database"),
	{Collection} = require("../dist/lib");

const table = new Collection("Table", {
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
			type: "ForeignId"
		}
	}, config);

beforeAll(async () => {
	await table.create();
});

afterAll(async () => {
	await table.drop();
});

describe("Table Collection", function() {
	
	let singleId;
	test("Insert", async () => {
		const [err, id] = await table.insert({
			name: "Test 101",
			description: "The long awaited moment is here! Hurray to the new horizon!",
			age: 18,
			grade: "81.90",
			isAlive: false,
			settings: {
				name: "The name",
				value: "The value"
			}
		});

		assert.isNull(err);
		assert.isTrue(id > 0);
		singleId = id;
	});

	test("InsertMany", async () => {
		const [err, ids] = await table.insertMany([
		{
			name: "Amira",
			age: 2,
			grade: 75
		},
		{
			name: "Awesome",
			age: 100,
			grade: 50
		}
		]);

		assert.isNull(err);
		assert.isArray(ids);
	});

	test("Update", async () => {
		const [err, done] = await table.update({Id: singleId, name: "Louose"}, {where: {Id: singleId}});

		assert.isNull(err);
		assert.isTrue(done);
	});

	test("Find", async () => {
		const [err, results] = await table.find();

		assert.isNull(err);
		assert.isArray(results);
		assert.isTrue(results.length === 3);
	});

	test("FindOne", async () => {
		const [err, value] = await table.findOne(false, {Id: singleId});

		assert.isNull(err);
		assert.isTrue("object" === typeof value);
	});

	test("getValue", async () => {
		const [err, value] = await table.getValue("name", {Id: singleId});

		assert.isNull(err);
		assert.isTrue(value === "Louose");
	});

	test("Delete", async () => {
		const [err, done] = await table.delete({where: {Id: singleId}});

		assert.isNull(err);
		assert.isTrue(done);
	});
});