const { ObjectId } = require('mongodb');
const mongo = require('./../routes/db');
var database;
mongo.connectToServer(function (err) {
  if (err) console.log(err);
  database = mongo.getDb()
});

async function addPost(req, res) {
  if (req.body.Post == undefined || (req.body.Post != undefined && req.body.Post == '')) {
    return res.send({ success: false, message: 'Missing or empty required parameters' })
  }

  var userId = ObjectId(req.appsession._id);
  var users = database.collection("users");

  users.findOne({ "_id": userId }, (error, user) => {
    if (user) {
      var feed = database.collection("feed");
      var data = {
        "UserId": userId.toString(),
        "UserName": user.Nickname,
        "Description": req.body.Post,
        "CreatedDate": Date.now(),
      };
      feed.insertOne(data, (error, result) => {
        if (error) {
          return res.status(500).send(error);
        }
        res.send({ success: true, message: "Post created successfully" })
      });
    } else {
      return res.status(500).send({ message: "User doesn't exist" });
    }
  });
}

async function addComment(req, res) {
  if (req.body.Comment == undefined || req.body.PostId == undefined
    || (req.body.Comment != undefined && req.body.Comment == '')
    || (req.body.PostId != undefined && req.body.PostId == '')) {
    return res.send({ success: false, message: 'Missing or empty required parameters' })
  }

  var userId = ObjectId(req.appsession._id);
  database.collection("users").findOne({ "_id": userId }, (error, user) => {
    if (user) {
      var feed = database.collection("feed");
      feed.findOne({ "_id": ObjectId(req.body.PostId) }, (error, feeddata) => {

        if (feeddata) {
          var data = {
            "UserId": userId.toString(),
            "PostId": req.body.PostId,
            "UserName": user.Nickname,
            "Description": req.body.Comment,
            "CreatedDate": Date.now(),
          };
          database.collection("comments").insertOne(data, (error, result) => {
            if (error) {
              return res.status(500).send(error);
            }
            res.send({ success: true, message: "Comment added successfully" });
          });
        } else {
          return res.send({ message: "The post you are commenting on doesn't exist anymore" });
        }
      });
    } else {
      return res.status(500).send({ message: "User doesn't exist" });
    }
  });
}

async function postActions(req, res) {
  if (req.body.Status === undefined || req.body.PostId === undefined || req.body.type === undefined
    || (req.body.PostId !== undefined && req.body.PostId == '') || (req.body.type !== undefined && req.body.type == '')) {
    return res.send({
      success: false, message: 'Missing or empty required parameters',
    })
  }

  if (['LIKE', 'HUG'].indexOf(req.body.type) === -1)
    return res.send({ success: false, message: 'Type is invalid' })

  if ([true, false].indexOf(req.body.Status) === -1)
    return res.send({ success: false, message: 'Status must be boolean' })

  var table = (req.body.type === 'LIKE') ? 'likes' : 'hugs';
  var userId = ObjectId(req.appsession._id);

  database.collection("users").findOne({ "_id": userId }, (error, user) => {
    if (user) {
      var feed = database.collection("feed");
      feed.findOne({ "_id": ObjectId(req.body.PostId) }, (error, feeddata) => {
        if (feeddata) {
          database.collection(table).findOne({ "PostId": req.body.PostId, "UserId": userId.toString() }, (error, actiondata) => {
            if (actiondata) { // If User already liked or Hug the post then update it else create
              database.collection(table).updateOne({ "_id": ObjectId(actiondata._id) }, { $set: { Status: req.body.Status } }, (error, result) => {
                if (error) {
                  return res.status(500).send(error);
                }
                actiondata.Status = req.body.Status;
                res.send({ success: true, message: "Status updated successfully", data: actiondata });
              });
            } else {
              var data = {
                "UserId": userId.toString(),
                "PostId": req.body.PostId,
                "UserName": user.Nickname,
                "Status": req.body.Status,
                "CreatedDate": Date.now(),
              };
              database.collection(table).insertOne(data, (error, result) => {
                if (error) {
                  return res.status(500).send(error);
                }
                res.send({ success: true, message: "Status added successfully", data: data });
              });
            }
          });
        } else {
          return res.send({ message: "The post you are commenting on doesn't exist anymore" });
        }
      });
    } else {
      return res.status(500).send({ message: "User doesn't exist" });
    }
  });
}

async function getFeed(req, res) {
  fdata = database.collection("feed").aggregate([

    {
      "$project": {
        UserName: 1,
        UserId: 1,
        Description: 1,
        CreatedDate: 1,
        _id: 0,
        "Fid": {
          "$toString": "$_id"
        }
      }
    },
    {
      $lookup: {
        from: "comments",
        localField: "Fid",
        foreignField: "PostId",
        as: "Comments"
      }
    },
    {
      $lookup: {
        from: "likes",
        localField: "Fid",
        foreignField: "PostId",
        as: "Likes"
      }
    },
    {
      $lookup: {
        from: "hugs",
        localField: "Fid",
        foreignField: "PostId",
        as: "Hugs"
      }
    },
    {
      $sort: { CreatedDate: -1 }
    }
  ]);

  fdata.toArray(function (error, feedData) {
    if (error) return res.status(500).send(error);
    return res.send({ success: true, data: feedData });
  });
}

module.exports = { addPost, addComment, postActions, getFeed }