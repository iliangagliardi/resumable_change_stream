const { MongoClient, ReadPreference, Timestamp } = require('mongodb');

const MONGO_URL = `mongodb+srv://....mongodb.net`;
const database = `mydb`;
const collection = `foo`;
const changeStreamTokenCollection = `${collection}.changeStreamToken`;
// manually or programmatically create the change stream capped collection for storing the last read token
// db.createCollection('foo.changeStreamToken',{capped:true, size:16000, max:1})


(async () => {
    const mongoClient = await MongoClient.connect(MONGO_URL, {
        appname: 'test-change-stream',
        readPreference: ReadPreference.PRIMARY,
        useNewUrlParser: true,
    });

    const db = await mongoClient.db(database);
    setup(changeStreamTokenCollection, db);


    // I also added this to show how you can resume at a specific point in time
    const desiredDate = new Date('2023-09-30T10:00');
    const currentTimestamp = new Timestamp({ t: Math.floor(desiredDate / 1000), i: 0 })
    const resumeByTimestamp = false


    // we can use a super small and fast capped collection to store our last token processed

    const resumeByLastProcessed = true
    const resumeAfterData = await db.collection(changeStreamTokenCollection).findOne()

    // some options
    const options = {
        'fullDocument': 'updateLookup'
    };


    // use this, and create the collection changeStreamRegistry (use the name you want)
    if (resumeByLastProcessed && resumeAfterData != null) {
        options["resumeAfter"] = {
            '_data': resumeAfterData._id,
        }
    }

    // use this if you want to resume from a specific timestamp
    if (resumeByTimestamp) {
        options["startAtOperationTime"] = currentTimestamp
    }

    const changeStream = db.collection(collection).watch([
        {
            '$match': {
                'operationType': { $in: ['insert', 'update', 'delete'] }
                // and you can filter by any data from your collection
            }
        },
        // { // only project out the fields you need 
        //     '$project': {
        //         "fullDocument.a":1,
        //         "fullDocument.b":1,
        //     }
        // }
    ],
        options
    );
    console.log("Change Stream started, awaiting for events...")
    changeStream.on('change', (event) => {
        // process the event however you want
        console.log(event);

        // store the last processed event in the capped collection
        //db.collection('changeStreamRegistry').insertOne({ '_id': event._id._data })
        insertToken(event._id._data, db)
    });

})();


async function insertToken(token, db) {
    await db.collection(changeStreamTokenCollection).insertOne({ '_id': token })
}


async function setup(cappedTokenColl, db) {
    const collections = await db.listCollections().toArray();
    const collectionExists = collections.some((col) => col.name === cappedTokenColl);

    if (!collectionExists) {
        await db.createCollection(cappedTokenColl, {capped:true, size:16000, max:1});
        console.log(`Collection '${cappedTokenColl}' created.`);
    } else {
        console.log(`Collection '${cappedTokenColl}' already exists.`);
    }
}
