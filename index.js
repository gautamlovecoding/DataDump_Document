import mongoose from 'mongoose';
const ObjectId = mongoose.Types.ObjectId;

async function connectAndCopyData(config = {}) {
    const options = {
        useCreateIndex: true,
        useNewUrlParser: true,
        useUnifiedTopology: true,
    };

    let sourceDB;
    let targetDB;

    try {
        const sourceConfig = config.sourceDatabase;
        const targetConfig = config.destinationDatabase;
        const allCollections = config.allCollection || false;
        const specifiedCollections = config.collectionsToCopy || [];
        const filterCondition = config.filterCondition;

        sourceDB = await mongoose.createConnection(sourceConfig, options);
        console.log(`Connected to source database..`);

        targetDB = await mongoose.createConnection(targetConfig, options);
        console.log(`Connected to target database..`);

        const sourceCollectionNames = await sourceDB.db.listCollections().toArray();

        const collectionsToCopy = allCollections
            ? sourceCollectionNames.map(collection => collection.name)
            : specifiedCollections;

        const copyPromises = collectionsToCopy.map(async (collectionName) => {
            const SourceModel = sourceDB.model(collectionName, new mongoose.Schema({}, { strict: false }));
            console.log(`Source Database Connected for ${collectionName}`);

            const TargetModel = targetDB.model(collectionName, new mongoose.Schema({}, { strict: false }));
            console.log(`Target Database Connected for ${collectionName}`);

            const dataToCopy = await SourceModel.find(filterCondition);

            console.log(`🚀 Data dump of collection: ${collectionName} successful`);

            await TargetModel.deleteMany(filterCondition);
            await TargetModel.insertMany(dataToCopy);
        });

        await Promise.all(copyPromises);

        console.log("Data Copied Successfully...");
    } catch (error) {
        console.error('An error occurred:', error);
        throw error; // Rethrow the error for the caller to handle
    } finally {
        if (sourceDB) {
            sourceDB.close();
        }
        if (targetDB) {
            targetDB.close();
        }
    }
}

let config = {
    sourceDatabase: 'mongodb://source-server/source-db',
    destinationDatabase: 'mongodb://target-server/target-db',
    collectionsToCopy: ['fiscalrankings', 'fiscalrankingmappers'],
    filterCondition: { ulb: { $in: [] } }
}

connectAndCopyData(config);
