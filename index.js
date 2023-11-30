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
    sourceDatabase: 'mongodb://gautam_kumar:ggJh2pPUlz9U3D@35.154.137.67:20595/perfect_010623?authsource=admin',
    destinationDatabase: 'mongodb://127.0.0.1:27017/cf_prod_local?directConnection=true',
    collectionsToCopy: ['fiscalrankings', 'fiscalrankingmappers'],
    filterCondition : { ulb : {$in: [ObjectId('5eb5845176a3b61f40ba08ac'), ObjectId("5fa2465e072dab780a6f11d4")]}}
}

connectAndCopyData(config);
