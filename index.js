

var MongoClient = require('mongodb').MongoClient;
module.exports = class MDB {



    Connect(dbName, connectedCB)
    {
      MongoClient.connect("mongodb://localhost:27017",{useNewUrlParser : true}).then((db)=> {
        this.mainDB = db;
        this.dbox = db.db(dbName);
        connectedCB();
      })
      .catch((error)=>{
        console.log("Error while connecting to mongoDB");
        console.log(error);
        connectedCB(error);
      });
    }

    Disconnect(disconnectedCB)
    {
      try{
        if(this.mainDB!=null)
        {
          if(this.mainDB.isConnected)
          {
            if(typeof disconnectedCB == "function")
              this.mainDB.close(true,disconnectedCB);
            else
            this.mainDB.close(true);
            this.mainDB = null;
            this.dbox = null;
          }
        }
      }
      catch(error)
      {
        console.log(error);
      }
    }

    FInsert(collection, objArray, doneCB)
    {
      if(this.dbox == undefined)
      {
        console.log("DB Insert error for the collection:");
        console.log(collection);
        console.log(" and the object is:")
        console.log(objArray);
        console.log("Details: DB did not opened, dbox is null.");
        return;
      }
      let bulk1 = this.dbox.collection(collection).initializeUnorderedBulkOp();

      if(Array.isArray(objArray))
      {
        for(let i in objArray)
        {
          bulk1.insert(objArray[i]);
        }
      }
      else if(typeof objArray  == "object")
      {
        bulk1.insert(objArray);
      }

      bulk1.execute((err0, result0) =>
      {
        if(err0) throw err0;
        if(typeof doneCB == "function")
          doneCB(result0);
      });
    }

    Insert(collection, objArray, override, doneCB)
    {
      if(this.dbox == undefined)
      {
        console.log("DB Insert error for the collection:");
        console.log(collection);
        console.log(" and the object is:")
        console.log(objArray);
        console.log("Details: DB did not opened, dbox is null.");
        return;
      }
      let bulk1 = this.dbox.collection(collection).initializeUnorderedBulkOp();

       if(override == false )
       {
            if(Array.isArray(objArray))
            {
                for(let i in objArray)
                {
                    bulk1.find({_id:objArray[i]._id}).upsert().updateOne({"$setOnInsert": objArray[i]})
                }
            }
            else if(typeof objArray  == "object")
            {
                bulk1.find({_id:objArray._id}).upsert().updateOne({"$setOnInsert": objArray})
            }

            bulk1.execute((err0, result0) =>
            {
                if(err0) throw err0;
                if(typeof doneCB == "function")
                doneCB(result0.result);
            });
        }
        else
        {
            if(Array.isArray(objArray))
            {
                for(let i in objArray)
                {
                    bulk1.find({_id:objArray[i]._id}).upsert().replaceOne(objArray[i]); 
                }
            }
            else if(typeof objArray  == "object")
            {
                bulk1.find({_id:objArray._id}).upsert().replaceOne(objArray); 
            }

            bulk1.execute((err0, result0) =>
            {
                if(err0) throw err0;
                if(typeof doneCB == "function")
                doneCB(result0.result);
            });
        }
    }

    Find(collection, query, cb, limit)
    {
      var x = query==undefined?{}:query;
      if(limit == undefined)
      {
        var limit = 100;
      }
      this.dbox.collection(collection).find(x).limit(limit).toArray((err, result)=>{
        if(err) throw err;
        cb(result);
      })
    }

    Remove(collection,query,  doneCB)
    {
      if(this.dbox == undefined)
      {
        console.log("DB Remove error for the collection:");
        console.log(collection);
        console.log("Details: DB did not opened, dbox is null.");
        return;
      }

      let bulk1 = this.dbox.collection(collection).initializeUnorderedBulkOp();

      bulk1.find(query).remove();
      bulk1.execute((err0, result0) =>
      {
        if(err0) throw err0;
        doneCB(result0);
      });
    }

    Upsert(collection, upsertObj, doneCB ) //{Query:{_id:"10", Updates:{$set: {armin:"salam"}}}
    {
      if(this.dbox == undefined)
      {
        console.log("DB Upsert error for the collection:");
        console.log(collection);
        console.log(" and the object is:")
        console.log(upsertObj);
        console.log("Details (Upsert): DB did not opened, dbox is null.");
        return;
      }
      let bulk1 = this.dbox.collection(collection).initializeUnorderedBulkOp();
      if(Array.isArray(upsertObj))
      {
        for(let i in upsertObj)
        {
            bulk1.find(upsertObj[i].Query).upsert().update(upsertObj[i].Updates);
        }
      }
      else if(typeof upsertObj  == "object")
      {
        bulk1.find(upsertObj.Query).upsert().update(upsertObj.Updates);
      }
      else
      {
        console.log("DB: Upsert Error in type of upsert command!");
      }

      bulk1.execute((err0, result0) =>
      {
        if(err0) throw err0;
        if(typeof doneCB == "function")
          doneCB(result0);
      });
    }


    Update(collection, upsertObj, doneCB ) //{Query:{_id:"10", Updates:{$set: {armin:"salam"}}}
    {
      if(this.dbox == undefined)
      {
        console.log("DB Upsert error for the collection:");
        console.log(collection);
        console.log(" and the object is:")
        console.log(upsertObj);
        console.log("Details (Upsert): DB did not opened, dbox is null.");
        return;
      }
      let bulk1 = this.dbox.collection(collection).initializeUnorderedBulkOp();
      if(Array.isArray(upsertObj))
      {
        for(let i in upsertObj)
        {
            bulk1.find(upsertObj[i].Query).update(upsertObj[i].Updates);
        }
      }
      else if(typeof upsertObj  == "object")
      {
        bulk1.find(upsertObj.Query).update(upsertObj.Updates);
      }
      else
      {
        console.log("DB: Upsert Error in type of upsert command!");
      }

      bulk1.execute((err0, result0) =>
      {
        if(err0) throw err0;
        if(typeof doneCB == "function")
          doneCB(result0);
      });
    }





    CopyCollection(sourceCollection, destCollectionName, cb) //OK
    {
        this.dbox.collection(sourceCollection).aggregate([ { $match: {} }, { $out: destCollectionName  } ],).hasNext().then(()=>{cb()}).catch((err)=>{if(err) throw err});
    }

    CopyCollectionToDB(sourceCollection, destDatabase, destCollection,cb) //OK
    {
        this.dbox.collection(sourceCollection).find().toArray((err, d)=>{ this,this.mainDB.db(destDatabase).collection(destCollection).insertMany(d).then(()=>{cb()}).catch((err)=>{throw err}); });
    }

    RenameCollection(collection, newName, cb) //you cannot rename a sharded collection, i will copy and drop! it is not efficient, it is time consumptor!
    {
        this.CopyCollection(collection, newName,()=>{
            this.dbox.collection(collection).dropIndexes().then(()=>{
                this.dbox.collection(collection).drop().then(()=>{
                    cb();
                }).catch((err)=>{
                   if(err) throw err;
                })

            }).catch((err)=>{
                if(err) throw err;
            })
        }) 
    }

    MoveCollectionToAnotherDB(collection, destDatabase, destCollectionName)
    {
        this.CopyCollectionToDB(collection,destDatabase, destCollectionName,()=>{
            this.dbox.collection(collection).dropIndexes().then(()=>{
                this.dbox.collection(collection).drop().then(()=>{
                    cb();
                }).catch((err)=>{
                   if(err) throw err;
                })

            }).catch((err)=>{
                if(err) throw err;
            })
        }) 
    }

    getCurrentDBName()
    {
        return this.dbox.databaseName;
    }









    Aggregation(collection, stages, cb)
    {
        if(this.dbox == undefined)
        {
          console.log("DB Upsert error for the collection:");
          console.log(collection);
          console.log(" and the object is:")
          console.log(upserts);
          console.log("Details (Upsert): DB did not opened, dbox is null.");
          return;
        }

      var res = null;
      if(Array.isArray(stages))
      {
        res = this.dbox.collection(collection).aggregate(stages);
      }
      else
      {
        res = this.dbox.collection(collection).aggregate([stages]);
      }

      res.toArray((err, result)=>{
        if(err) throw err;
        cb(result);
      })

        
    }


}
  