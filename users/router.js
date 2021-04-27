const bcrypt = require('bcrypt'),
      crypto = require('crypto');
const moment = require('moment');
const ObjectID = require('mongodb').ObjectID;

const GenerateSalt = (bytes = 28) => crypto.randomBytes(bytes).toString('base64');
const Encrypt = (password, salt) => crypto.pbkdf2Sync(password, salt, 20000, 64, 'sha512').toString('base64');

module.exports = function(Wrapper) {
  const Prefetch = Wrapper.Prefetch;
  const Router = this;

  let AddRoute = require('../addRouteFactory')(Wrapper, Router);

  AddRoute('getByID', data => new Promise(async resolve => {
    if(!data || !data.SID) return resolve({success: false, message: 'No SID provided'});
    if(Wrapper.FetchedIDs[data.SID]) return resolve({success: true, item: Wrapper.FetchedIDs[data.SID]});
    Wrapper.DB.findOne({SID: data.SID}, async (err, dbResponse) => {
      if(err) return resolve({success: false, message: err.message});
      if(!dbResponse) return resolve({success: false, message: 'User not found'});
      let fetchedItem = await Wrapper.Add(dbResponse);
      resolve({success: true, item: fetchedItem});
    })
  }))

  AddRoute('search', data => new Promise(async resolve => {
    if(!data || !data.Query) return resolve({success: false, message: 'No Query provided'});
    if(Wrapper.FetchedIDs[data.Query]) {
      let {SID, Username, Email} = Wrapper.FetchedIDs[data.Query];
      return resolve({success: true, items: [{SID, Username, Email}]});
    }
    Wrapper.DB.find({$or: [{SID: data.Query}, {Email: data.Query}, {Username: data.Query}]}, {fields: {SID: 1, Username: 1, Email: 1}}).toArray(async (err, dbResponse) => {
      if(err) return resolve({success: false, message: err.message});
      resolve({success: true, items: dbResponse});
    })
  }))

  AddRoute('getFreeUsersSupportData', data => new Promise(async resolve => {
    //let Total = await Wrapper.DB.countDocuments({RegistrationType: 'free', SID: {$regex: new RegExp(`^${data.SID}$`, 'i')}} );
    console.log('Inside getFreeUsersSupportData');
    console.log('SID:'+ data.SID);
    let SID = data.SID || '';
    //RegistrationType: 'free', 
    Wrapper.DB.find({RegistrationType: 'free', SID: {$regex: new RegExp(`^${SID}`, 'i')}}, {_id: 0, SID: 1, Username: 1, DateJoined: 1, DateLastRenewed: 1, Active: 1}).toArray((errors, dbResponse) => {
      console.log('Items Length before filter:'+ dbResponse.length);
      let Total = dbResponse.length;
      let Items = dbResponse;
      if(Items.length>1) Items = dbResponse.filter((_,i) => (i > (data.Skip || 0)) && (i <= (data.Skip || 0)+(data.Limit || 10)));
      console.log('User total records:'+ Items.length);
      resolve({success: true, Items, Total});
    })
  }))


  AddRoute('getTimeEntrySupportData', data => new Promise(async resolve => {
    let monetized = !!data.Monetized;

    let Sort = (a,b) => a.TimeHistory.DateCreated - b.TimeHistory.DateCreated;
    if(data.SortMode == 'MostHours') Sort = (a,b) => b.TimeHistory.map(th => th.Hours).reduce((a,b)=>a+b) - a.TimeHistory.map(th => th.Hours).reduce((a,b)=>a+b)

    let dateStart = moment().add(monetized ? 0 : -1, 'weeks').startOf('week').toDate();
    let dateEnd = moment().add(monetized ? 0 : -1, 'weeks').endOf('week').toDate();

    Wrapper.DB.find({TimeHistory: {$elemMatch: {Status: 'pending', DateCreated: {$gt: dateStart, $lt: dateEnd}}}}, {fields: {_id: 0, SID: 1, Username: 1, TimeHistory: 1, RankIndexMonetized: 1}}).toArray((errors, dbResponse) => {
      let Items = dbResponse.map(item => {
        
        let MaxMonetizedHours = [0,0,0,5,6,7,12,15,17,18,20,22,24,28,30,35,40,][item.RankIndexMonetized];

        let hoursCounted = 0;
        let validHistoryEntries = item.TimeHistory
                                  .filter(tentry => tentry.Status == 'pending' && (tentry.DateCreated-0) >= (dateStart-0) && (tentry.DateCreated-0) < (dateEnd-0))
                                  .map(entry => {
                                    if(!monetized) {
                                      if(hoursCounted < MaxMonetizedHours) return false;
                                      entry.OriginalHours = entry.Hours;
                                      entry.Hours = Math.max(MaxMonetizedHours - hoursCounted, entry.Hours);
                                    } else {
                                      if(hoursCounted >= MaxMonetizedHours) return false;
                                      entry.OriginalHours = entry.Hours;
                                      entry.Hours = Math.min(MaxMonetizedHours - hoursCounted, entry.Hours);
                                    }

                                    hoursCounted += entry.Hours;
                                    return entry;
                                  })
                                  .filter(e => !!e);

        item.TimeHistory = validHistoryEntries;

        if(data.SortMode == 'MostHours') item.TimeHistory = item.TimeHistory.sort((a,b) => b.Hours - a.Hours);
        if(hoursCounted == 0) return false;
        return item;
      }).filter(e => !!e).sort(Sort);

      let Total = Items.length;
      
      Items = Items.filter((_,i) => (i > (data.Skip || 0)) && (i <= (data.Skip || 0)+(data.Limit || 20)))

      resolve({success: true, Items, Total});
    })
  }))

  AddRoute('invalidateBySID', data => new Promise(async resolve => {
    if(!data.SID) return resolve({success: false, message: 'No SID provided'});
    if(Wrapper.FetchedIDs && Wrapper.FetchedIDs[data.SID]) {
      Wrapper.FetchedIDs[data.SID] = undefined;
    }
    await MDB.collection('sessions').updateMany({"session.userData.SID": this.SID}, {
      $set: {"session.LastCache": 0}
    })
    return resolve({success: true});
  }))

  AddRoute('invalidateManyBySID', data => new Promise(async resolve => {
    if(!data.SIDs) return resolve({success: false, message: 'No SIDs provided'});
    for(let i = 0; i < data.SIDs.length; i++) {
      let SID = data.SIDs[i];
      if(Wrapper.FetchedIDs && Wrapper.FetchedIDs[data.SID]) {
        Wrapper.FetchedIDs[data.SID] = undefined;
      }
      await MDB.collection('sessions').updateMany({"session.userData.SID": this.SID}, {
        $set: {"session.LastCache": 0}
      })
    }
    return resolve({success: true});
  }))

  AddRoute('getNamesBySID', data => new Promise(async resolve => {
    if(!data || !data.SID) return resolve({success: false, message: 'No ID specified'});
    if(Wrapper.FetchedIDs[data.SID]) {
      let member = Wrapper.FetchedIDs[data.SID];
      let outNames = `${member.Username}`;
      return resolve({success: true, Names: outNames});
    }
    Wrapper.DB.findOne({$or: [{SID: data.SID}, {Email: data.SID}]}, async (errors, dbResponse) => {
      if(!dbResponse) return resolve({success: false, message: 'User not found'})
      let fetchedItem = await Wrapper.Add(dbResponse);
      let outNames = `${fetchedItem.Username}`;
      return resolve({success: true, Names: outNames});
    })
  }))

  AddRoute('login', data => new Promise(async resolve => {
    if(!data || !data.SID) return resolve({success: false, message: 'No SID provided'});
    if(!data.Password) return resolve({success: false, message: 'No Password provided'});

    let attemptLogin = async function(userItem) {
      if(userItem.Terminated) return {success: false, message: 'Account Terminated'};
      if(userItem.Suspended) return {success: false, message: 'Account Suspended'};
      if((userItem.RegistrationType == 'free') && (moment().add(-60, 'days').startOf('day').isAfter(moment(userItem.DateJoined).startOf('day')))) return {success: false, message: 'Account Terminated'};

      if(userItem.PasswordEngine == 0) {
        let success = bcrypt.compareSync(data.Password, userItem.PasswordHash.replace(/^\$2y(.+)$/i, '$2a$1'));
        if(!success) return {success: false, message: 'Invalid Password'}
        
        let PasswordSalt = GenerateSalt();
        let PasswordHash = Encrypt(data.Password, PasswordSalt);
        
        Wrapper.DB.updateOne({SID: data.SID}, {$set: {
          PasswordHash,
          PasswordSalt,
          PasswordEngine: 1,
          DateLastLogin: new Date(),
        }})

        return {success: true, SID: userItem.SID, IsMonetized: userItem.Monetized, IsActive: (moment(userItem.DateExpiring).diff() > 0)}
      }
      if(userItem.PasswordEngine == 1) {
        let hashVerify = Encrypt(data.Password, userItem.PasswordSalt);
        let success = userItem.PasswordHash == hashVerify;

        Wrapper.DB.updateOne({SID: data.SID}, {$set: {
          DateLastLogin: new Date(),
        }})
        return {success, message: success ? 'Correct Password' : 'Invalid Password', SID: userItem.SID, IsMonetized: userItem.Monetized, IsActive: (moment(userItem.DateExpiring).diff() > 0)};
      }
    }

    Wrapper.DB.findOne({$or: [{SID: data.SID}, {Username: {$regex: new RegExp(`^${data.SID}$`, 'i')}}]}, async (errors, dbResponse) => {
      if(errors || !dbResponse) return resolve({success: false, message: `User with the ID or Username "${data.SID}" not found`});
      let fetchedItem = await Wrapper.Add(dbResponse);
      return resolve(attemptLogin(fetchedItem));
    })
  }))

  AddRoute('suspend', data => new Promise(async resolve => {
    if(!data || !data.SID) return resolve({success: false, message: 'No SID provided'});
    if(!data.Reason) return resolve({success: false, message: 'No Reason provided'});
    if(!data.Admin) return resolve({success: false, message: 'No Admin provided'});
    
    Wrapper.DB.updateOne({SID: data.SID}, {
      $set: {
        Suspended: true,
        Active: false,
      },
      $push: {
        SuspensionHistory: {
          Suspended: true,
          Date: new Date(),
          Admin: data.Admin,
          Reason: data.Reason,
        }
      }
    }, (err, results) => {
      if(err) return resolve({success: false, message: err.message});
      
      MDB.collection('genealogy').updateOne({SID: data.SID}, {
        $set: {
          Active: false,
        }
      }, (err, results) => {
        if(err) return resolve({success: false, message: err.message});

        return resolve({success: true});
      });

    })
  }))

  AddRoute('unsuspend', data => new Promise(async resolve => {
    if(!data || !data.SID) return resolve({success: false, message: 'No SID provided'});
    if(!data.Reason) return resolve({success: false, message: 'No Reason provided'});
    if(!data.Admin) return resolve({success: false, message: 'No Admin provided'});

    Wrapper.DB.updateOne({SID: data.SID}, {
      $set: {
        Suspended: false,
        Active: true,
      },
      $push: {
        SuspensionHistory: {
          Suspended: false,
          Date: new Date(),
          Admin: data.Admin,
          Reason: data.Reason,
        }
      }
    }, (err, results) => {
      if(err) return resolve({success: false, message: err.message});
      
      MDB.collection('genealogy').updateOne({SID: data.SID}, {
        $set: {
          Active: true,
        }
      }, (err, results) => {
        if(err) return resolve({success: false, message: err.message});

        return resolve({success: true});
      });

    })
  }))

  AddRoute('terminate', data => new Promise(async resolve => {
    if(!data || !data.SID) return resolve({success: false, message: 'No SID provided'});
    if(!data.Reason) return resolve({success: false, message: 'No Reason provided'});
    if(!data.Admin) return resolve({success: false, message: 'No Admin provided'});
    
    let user = await Wrapper.DB.findOne({SID: data.SID});
    if(!user) return resolve({success: false, message: 'User not found'});

    let changes = {
      Terminated: true,
      TerminationReason: data.Reason,
      TerminationAdmin: data.Admin,
      Active: false,
    };
    
    if(!data.LockFields) {
      changes.Username = `was: ${user.Username}`;
      changes.Email = `was: ${user.Email}`;
      changes.Mobile = `was: ${user.Mobile}`;
    }

    Wrapper.DB.updateOne({SID: data.SID}, {
      $set: changes,
      $push: {
        SuspensionHistory: {
          Terminated: true,
          Date: new Date(),
          Admin: data.Admin,
          Reason: data.Reason,
        }
      }
    }, (err, results) => {
      if(err) return resolve({success: false, message: err.message});
      
      MDB.collection('genealogy').updateOne({SID: data.SID}, {
        $set: {
          Active: false,
        }
      }, (err, results) => {
        if(err) return resolve({success: false, message: err.message});

        return resolve({success: true});
      });

    })
  }))

  AddRoute('modifyDataPoints', data => new Promise(async resolve => {
    if(!data || !data.SID) return resolve({success: false, message: 'No SID provided'});
    if(!data.Amount) return resolve({success: false, message: 'No amount specified'});
    if(!data.Notes) return resolve({success: false, message: 'No notes specified'});
    if(!data.Reason) return resolve({success: false, message: 'No reason specified'});
    let user = await Wrapper.DB.findOne({SID: data.SID});
    if(!user) return resolve({success: false, message: 'A user with the specified SID was not found'});

    data.Amount = data.Amount-0;
    
    if(user.Bank.DataPoints.Balance + data.Amount < 0 && !data.AllowBelowZero) return resolve({success: false, message: `You don't have enough Data Points`});

    let historyEntry = {
      Amount: data.Amount,
      Reason: data.Reason,
      Date: new Date(),
      Approved: true,
      DateApproved: new Date(),
      Notes: data.Notes,
    };

    let response = await Wrapper.DB.updateOne({SID: data.SID}, {$inc: {"Bank.DataPoints.Balance": data.Amount}, $push: {"Bank.DataPoints.History": historyEntry}})
    let success = response.modifiedCount == 1;
    if(success && Wrapper.FetchedIDs[data.SID]) {
      Wrapper.FetchedIDs[data.SID].Bank.DataPoints.Balance += data.Amount;
      Wrapper.FetchedIDs[data.SID].Bank.DataPoints.History.push(historyEntry);
    }

    resolve({success})
  }))

  AddRoute('modifyRenewalPoints', data => new Promise(async resolve => {
    if(!data || !data.SID) return resolve({success: false, message: 'No SID provided'});
    if(!data.Amount) return resolve({success: false, message: 'No amount specified'});
    if(!data.Notes) return resolve({success: false, message: 'No notes specified'});
    if(!data.Reason) return resolve({success: false, message: 'No reason specified'});
    let user = await Wrapper.DB.findOne({SID: data.SID});
    if(!user) return resolve({success: false, message: 'A user with the specified SID was not found'});

    data.Amount = data.Amount-0;

    if(user.Bank.RenewalPoints + data.Amount < 0 && !data.AllowBelowZero) return resolve({success: false, message: `You don't have enough Renewal Points`});

    let historyEntry = {
      Amount: data.Amount,
      Reason: data.Reason,
      Date: new Date(),
      Approved: true,
      DateApproved: new Date(),
      Notes: data.Notes,
    };

    let response = await Wrapper.DB.updateOne({SID: data.SID}, {$inc: {"Bank.RenewalPoints": data.Amount}, $push: {"Bank.RenewalHistory": historyEntry}})
    let success = response.modifiedCount == 1;
    if(success && Wrapper.FetchedIDs[data.SID]) {
      Wrapper.FetchedIDs[data.SID].Bank.RenewalPoints += data.Amount;
      if(!Wrapper.FetchedIDs[data.SID].Bank.RenewalHistory) Wrapper.FetchedIDs[data.SID].Bank.RenewalHistory = [];
      Wrapper.FetchedIDs[data.SID].Bank.RenewalHistory.push(historyEntry);
  }

    resolve({success})
  }))

  AddRoute('modifyeWallet', data => new Promise(async resolve => {
    if(!data || !data.SID) return resolve({success: false, message: 'No SID provided'});
    if(!data.Amount) return resolve({success: false, message: 'No amount specified'});
    if(!data.Notes) return resolve({success: false, message: 'No notes specified'});
    if(!data.Reason) return resolve({success: false, message: 'No reason specified'});
    let user = await Wrapper.DB.findOne({SID: data.SID});
    if(!user) return resolve({success: false, message: 'A user with the specified SID was not found'});

    data.Amount = data.Amount-0;

    if(user.Bank.eWallet.Balance + data.Amount < 0 && !data.AllowBelowZero) return resolve({success: false, message: `You don't have enough eWallet Points`});

    let historyEntry = {
      Amount: data.Amount,
      Reason: data.Reason,
      Date: new Date(),
      Approved: true,
      DateApproved: new Date(),
      Notes: data.Notes,
    };

    let response = await Wrapper.DB.updateOne({SID: data.SID}, {$inc: {"Bank.eWallet.Balance": data.Amount}, $push: {"Bank.eWallet.History": historyEntry}})
    let success = response.modifiedCount == 1;
    if(success && Wrapper.FetchedIDs[data.SID]) {
      Wrapper.FetchedIDs[data.SID].Bank.eWallet.Balance += data.Amount;
      Wrapper.FetchedIDs[data.SID].Bank.eWallet.History.push(historyEntry);
    }

    resolve({success})
  }))

  AddRoute('modifyWalletGeneric', data => new Promise(async resolve => {
    if(!data || !data.SID) return resolve({success: false, message: 'No SID provided'});
    if(!data.Amount) return resolve({success: false, message: 'No amount specified'});
    if(!data.Notes) return resolve({success: false, message: 'No notes specified'});
    if(!data.Reason) return resolve({success: false, message: 'No reason specified'});
    if(!data.Wallet) return resolve({success: false, message: 'No wallet specified'});
    let user = await Wrapper.DB.findOne({SID: data.SID});
    if(!user) return resolve({success: false, message: 'A user with the specified SID was not found'});

    data.Amount = data.Amount-0;

    if((user.Bank[data.Wallet] || {Balance: 0}).Balance + data.Amount < 0 && !data.AllowBelowZero) return resolve({success: false, message: `You don't have enough eWallet Points`});

    let historyEntry = {
      Amount: data.Amount,
      Reason: data.Reason,
      Date: new Date(),
      Approved: true,
      DateApproved: new Date(),
      Notes: data.Notes,
    };

    let response = await Wrapper.DB.updateOne({SID: data.SID}, {$inc: {[`Bank.${data.Wallet}.Balance`]: data.Amount}, $push: {[`Bank.${data.Wallet}.History`]: historyEntry}})
    let success = response.modifiedCount == 1;
    if(success && Wrapper.FetchedIDs[data.SID]) {
      Wrapper.FetchedIDs[data.SID].Bank.eWallet.Balance += data.Amount;
      Wrapper.FetchedIDs[data.SID].Bank.eWallet.History.push(historyEntry);
    }
    
    await Wrapper.fn.invalidateBySID({SID: user.SID});

    resolve({success})
  }))

  AddRoute('changePassword', data => new Promise(async resolve => {
    if(!data ||!data.SID) return resolve({success: false, message: 'No SID provided'});
    if(!data.RequestByAdmin && !data.OldPassword) return resolve({success: false, message: 'No old password provided'});
    if(!data.NewPassword) return resolve({success: false, message: 'No new password provided'});
    if(!data.NewPasswordConfirm) return resolve({success: false, message: 'No new password confirm provided'});
    if(data.NewPassword !== data.NewPasswordConfirm) return resolve({success: false, message: 'New password confirm does not match'});

    Wrapper.DB.findOne({SID: data.SID}, async (errors, dbResponse) => {
      if(errors || !dbResponse) return resolve({success: false, message: `User with the ID or Username "${data.SID}" not found`});

      let user = dbResponse;
      let success = data.RequestByAdmin || (user.PasswordHash == Encrypt(data.OldPassword, user.PasswordSalt));

      if(!success) return resolve({success: false, message: 'Old password does not match'});

      let PasswordSalt = GenerateSalt();
      let PasswordHash = Encrypt(data.NewPassword, PasswordSalt);
      
      Wrapper.DB.updateOne({SID: data.SID}, {$set: {
        PasswordHash,
        PasswordSalt,
        PasswordEngine: 1,
        DatePasswordChanged: new Date(),
      }})

      return resolve({success: true});
    })
  }))

  AddRoute('findByEmail', data => new Promise(async resolve => {
    if(!data || !data.Email) return resolve({success: false, message: 'No Email provided'});

    let user = await Wrapper.DB.findOne({Email: data.Email});
    if(!user) return resolve({success: false, message: `User not found`});
    
    return resolve({success: true, item: user});
  }))

  AddRoute('addResetToken', data => new Promise(async resolve => {
    if(!data || !data.SID) return resolve({success: false, message: 'No SID provided'});
    if(!data || !data.Token) return resolve({success: false, message: 'No Token provided'});

    let user = await Wrapper.DB.findOne({SID: data.SID});
    if(!user) return resolve({success: false, message: `User not found`});
    
    await Wrapper.DB.updateOne({SID: user.SID}, {
      $push: {
        ResetTokens: {
          Token: data.Token,
          Expiration: moment().add(1, 'hour').toDate(),
        }
      }
    })

    return resolve({success: true, item: user});
  }))

  AddRoute('resetPassword', data => new Promise(async resolve => {
    if(!data || !data.SID) return resolve({success: false, message: 'No SID provided'});
    if(!data || !data.Token) return resolve({success: false, message: 'No Token provided'});
    if(!data || !data.NewPassword) return resolve({success: false, message: 'No Password provided'});
    if(!data || !data.NewPasswordConfirm) return resolve({success: false, message: 'No Password Confirmation provided'});
    if(data.NewPassword !== data.NewPasswordConfirm) return resolve({success: false, message: `Password Confirmation does not match`});
    data.NewPassword = String(data.NewPassword);
    if(data.NewPassword.length <= 7) return resolve({success: false, message: `Your new password needs to contain at least 8 characters`})

    let user = await Wrapper.DB.findOne({SID: data.SID});
    if(!user) return resolve({success: false, message: `User not found`});

    let token = (user.ResetTokens || []).find(t => t.Token == data.Token);
    if(!token) return resolve({success: false, message: `An invalid reset token was used. Please request another one`});
    if(moment(token.Expiration).diff() < 0) {
      return resolve({success: false, message: `The reset token has expired. Please request another one`})
    }

    let PasswordSalt = GenerateSalt();
    let PasswordHash = Encrypt(data.NewPassword, PasswordSalt);

    Wrapper.DB.updateOne({SID: data.SID}, {
      $set: {
        PasswordHash,
        PasswordSalt,
        PasswordEngine: 1,
        DatePasswordChanged: new Date(),
      }, 
      $pull: {
        ResetTokens: {
          Token: data.Token
        }
      }
    })

    return resolve({success: true});
  }))

  AddRoute('extendExpiration', data => new Promise(async resolve => {
    if(!data ||!data.SID) return resolve({success: false, message: 'No SID provided'});
    Wrapper.DB.updateMany(
      {SID: data.SID},
      [{ $set: { DateExpiring: { $add: ["$DateExpiring", 28*24*60*60000] }, "Bank.RenewalPoints": { $add: ["$Bank.RenewalPoints", -99] } } }]
    )
  }))

  AddRoute('extendExpirationAdmin', data => new Promise(async resolve => {
    if(!data ||!data.SID) return resolve({success: false, message: 'No SID provided'});
    let user = await Wrapper.DB.findOne({SID: data.SID});
    if(!user) return resolve({success: false, message: 'User not found'});
    let days = data.Days || 28;

    // Go Green Expiration Catch-Up
    if(!user.Monetized || moment().diff(moment(user.DateExpiring).endOf('day')) > 0) {
      user.DateExpiring = moment().startOf('day').toDate();
    }
    // Go Green Expiration Catch-Up
    
    let newDateExpiring = (user.RegistrationType == 'free') ? (moment().startOf('day').add(days, 'days').toDate()) : (new Date((user.DateExpiring-0) + days*24*60*60000));

    let active = (moment().diff(newDateExpiring) < 0);

    let newValues = {
      DateExpiring: newDateExpiring,
      RegistrationType: 'premium',
      Active: active
    };

    Wrapper.DB.updateMany(
      {SID: data.SID},
      {
        $set: newValues,
        $push: {
          AdminExtensions: {
            AdminSID: data.AdminSID,
            Date: new Date(),
          },
          RenewalHistory: {Date: new Date(), Method: data.AdminSID, Extension: `${days} Days`}
        },
      },
      (err, response) => {
        if(err) return resolve({success: false, message: err.message});
        MDB.collection('genealogy').updateOne({SID: data.SID}, {
          $set: newValues
        }, async (err, results) => {
          if(err) return resolve({success: false, message: err.message})

          if(data.IsMonetized) {
            await Wrapper.fn.monetize({SID: user.SID, AdminSID: 'platform'});
          }
          
          await Wrapper.fn.invalidateBySID({SID: user.SID});
  
          return resolve({success: true});
        });
      }
    )
  }))

  AddRoute('reduceExpirationAdmin', data => new Promise(async resolve => {
    if(!data ||!data.SID) return resolve({success: false, message: 'No SID provided'});
    let user = await Wrapper.DB.findOne({SID: data.SID});
    if(!user) return resolve({success: false, message: 'User not found'});
    let newDateExpiring = (new Date((user.DateExpiring-0) - 28*24*60*60000));
    let active = (moment().diff(newDateExpiring) < 0);
    Wrapper.DB.updateMany(
      {SID: data.SID},
      {
        $set: {
          DateExpiring: newDateExpiring,
          RegistrationType: 'premium',
          Active: active
        },
        $push: {
          AdminExtensions: {
            AdminSID: data.AdminSID,
            Date: new Date(),
            IsReduction: true
          }
        },
      },
      (err, response) => {
        if(err) return resolve({success: false, message: err.message});
        MDB.collection('genealogy').updateOne({SID: data.SID}, {
          $set: {
            Active: active,
            DateExpiring: newDateExpiring,
            RegistrationType: 'premium',
          }
        }, (err, results) => {
          if(err) return resolve({success: false, message: err.message})
  
          return resolve({success: true});
        });
      }
    )
  }))

  AddRoute('automaticRenewalProcess', data => new Promise(async resolve => {
    Wrapper.DB.updateMany(
      {"Bank.RenewalPoints": {$gte: 99}},
      [{ $set: { DateExpiring: { $add: ["$DateExpiring", 28*24*60*60000] }, "Bank.RenewalPoints": { $add: ["$Bank.RenewalPoints", -99] } } }]
    )
  }))

  AddRoute('addTimeEntry', data => new Promise(async resolve => {
    if(!data || !data.SID) return resolve({success: false, message: 'No SID provided'});
    if(!data.Type) return resolve({success: false, message: 'No Type provided'});
    if(!data.Description) return resolve({success: false, message: 'No Description provided'});
    if(!data.Hours) return resolve({success: false, message: 'No Hours provided'});
    
    let user = Wrapper.FetchedIDs[data.SID] || await Wrapper.DB.findOne({SID: data.SID});
    if(!user) return resolve({success: false, message: 'User not found'});
    if(!Wrapper.FetchedIDs[data.SID]) user = await Wrapper.Add(user);
    let rankIndex = data.CurrentRankIndex || user.RankIndex;
    let hoursDone = user.TimeHistory.filter(th => (new Date(th.DateCreated) > moment(data.Date || Date.now()).startOf('week').toDate()) && (new Date(th.DateCreated) < moment(data.Date || Date.now()).endOf('week').toDate())).map(th => th.Hours);
    hoursDone = [hoursDone, 0].flat().reduce((a,b) => a+b)
    let hoursAvailable = [0,0,0,5,6,7,12,15,17,18,20,22,24,28,30,35,40,][rankIndex] - hoursDone;
    if(hoursAvailable <= 0) return resolve({success: false, message: 'No available hours remaining'});
    if(hoursAvailable < (data.Hours - 0)) return resolve({success: false, message: 'Not enough hours remaining for this week for your current service level'});
    if(data.Hours == 0) return resolve({success: false, message: 'Please submit more than 0 hours'});

    let entry = {
      Description: data.Description,
      AdminComment: null,
      Admin: null,
      Status: 'pending',
      Type: data.Type,
      Hours: (data.Hours - 0) ?? 0,
      RankIndex: rankIndex,
      DateModified: data.Date || new Date(),
      DateCreated: data.Date || new Date(),
    }

    let response = await Wrapper.DB.updateMany({SID: data.SID}, {
      $push: {
        TimeHistory: entry
      }
    });

    user.TimeHistory.push(entry);

    Wrapper.FetchedIDs[data.SID] = user;

    return resolve({success: true, item: response});
  }))

  AddRoute('removeTimeEntry', data => new Promise(async resolve => {
    if(!data.SID) return resolve({success: false, message: 'No SID provided'});
    if(!data.Date) return resolve({success: false, message: 'No Date provided'});

    Wrapper.DB.findOne({SID: data.SID}, (err, user) => {
      let itemToChange = (user.TimeHistory ?? []).filter(th => ((th.DateCreated-0) == (data.Date-0)));
      if(!itemToChange) return resolve({success: false, message: 'Time entry item not found'});
      Wrapper.DB.updateOne(
        {
          SID: data.SID
        },
        {
          $pull: {
            TimeHistory: {DateCreated: new Date(data.Date)}
          }
        }
      )
      .then(response => {
        if(Wrapper.FetchedIDs[data.SID]) {
          Wrapper.FetchedIDs[data.SID].TimeHistory = Wrapper.FetchedIDs[data.SID].TimeHistory.filter(th => !(((th.DateCreated-0) == (new Date(data.Date)-0))))
        }

        return resolve({success: true});
      })
    })
  }))

  AddRoute('approveTimeEntry', data => new Promise(async resolve => {
    if(!data.SID) return resolve({success: false, message: 'No SID provided'});
    if(!data.Date) return resolve({success: false, message: 'No Date provided'});
    Wrapper.DB.findOne({SID: data.SID}, (err, user) => {
      let itemToChange = (user.TimeHistory ?? []).filter(th => th.DateCreated-0 == data.Date-0);
      console.log("Under approveTimeEntry #538");
      console.log("SID:"+ data.SID);
      console.log("Reason:"+ data.Reason);
      if(!itemToChange) return resolve({success: false, message: 'Time entry item not found'});
      Wrapper.DB.updateOne({SID: data.SID},
      {
        $set: {
          "TimeHistory.$[n].Status": 'confirm',
          "TimeHistory.$[n].Admin": data.Admin,
          "TimeHistory.$[n].AdminComment": data.AdminComment,
          "TimeHistory.$[n].DateModified": new Date(),
          "TimeHistory.$[n].Reason": data.Reason
        }
      },
      {
        arrayFilters: [
          {
            "n.DateCreated": new Date(data.Date-0)
          }
        ]
      })
      .then(response => {
        return resolve({success: true});
      })
    })
  }))

  AddRoute('denyTimeEntry', data => new Promise(async resolve => {
    if(!data.SID) return resolve({success: false, message: 'No SID provided'});
    if(!data.Date) return resolve({success: false, message: 'No Date provided'});
    Wrapper.DB.findOne({SID: data.SID}, (err, user) => {
      let itemToChange = (user.TimeHistory ?? []).filter(th => th.DateCreated-0 == data.Date-0);
      if(!itemToChange) return resolve({success: false, message: 'Time entry item not found'});
      Wrapper.DB.updateOne({SID: data.SID},
      {
        $set: {
          "TimeHistory.$[n].Status": 'denied',
          "TimeHistory.$[n].Admin": data.Admin,
          "TimeHistory.$[n].AdminComment": data.AdminComment,
          "TimeHistory.$[n].DateModified": new Date(),
        }
      },
      {
        arrayFilters: [
          {
            "n.DateCreated": new Date(data.Date-0)
          }
        ]
      })
      .then(response => {
        return resolve({success: true});
      })
    })
  }))

  AddRoute('approveUserTimeEntries', data => new Promise(async resolve => {
    if(!data.SID) return resolve({success: false, message: 'No SID provided'});
    Wrapper.DB.findOne({SID: data.SID}, (err, user) => {
      let itemToChange = (user.TimeHistory ?? []).filter(th => th.DateCreated-0 == data.Date-0);
      console.log("Under approveUserTimeEntries #593");
      console.log("SID:"+ data.SID);
      console.log("Reason:"+ data.Reason);
      if(!itemToChange) return resolve({success: false, message: 'Time entry item not found'});
      Wrapper.DB.updateOne({SID: data.SID},
      {
        $set: {
          "TimeHistory.$[n].Status": 'confirm',
          "TimeHistory.$[n].Admin": data.Admin,
          "TimeHistory.$[n].AdminComment": data.AdminComment,
          "TimeHistory.$[n].DateModified": new Date(),
          "TimeHistory.$[n].Reason": data.Reason,
        }
      },
      {
        arrayFilters: [
          {
            "n.Status": 'pending',
            "n.DateCreated": {$lt: moment().startOf('week').toDate()}
          }
        ]
      })
      .then(response => {
        return resolve({success: true});
      })
    })
  }))

  AddRoute('denyUserTimeEntries', data => new Promise(async resolve => {
    if(!data.SID) return resolve({success: false, message: 'No SID provided'});
    Wrapper.DB.findOne({SID: data.SID}, (err, user) => {
      let itemToChange = (user.TimeHistory ?? []).filter(th => th.DateCreated-0 == data.Date-0);
      if(!itemToChange) return resolve({success: false, message: 'Time entry item not found'});
      Wrapper.DB.updateOne({SID: data.SID},
      {
        $set: {
          "TimeHistory.$[n].Status": 'denied',
          "TimeHistory.$[n].Admin": data.Admin,
          "TimeHistory.$[n].AdminComment": data.AdminComment,
          "TimeHistory.$[n].DateModified": new Date(),
        }
      },
      {
        arrayFilters: [
          {
            "n.Status": 'pending',
            "n.DateCreated": {$lt: moment().startOf('week').toDate()}
          }
        ]
      })
      .then(response => {
        return resolve({success: true});
      })
    })
  }))

  AddRoute('updateMultiTimeEntries', data => new Promise(async resolve => {
    if(!data.Status) return resolve({success: false, message: 'No Status provided'});
    if(!data.Admin) return resolve({success: false, message: 'No Admin provided'});
    if(!data.AdminComment) return resolve({success: false, message: 'No AdminComment provided'});
    if(!data.EntriesPerSID) return resolve({success: false, message: 'No EntriesPerSID provided'});

    if(typeof data.EntriesPerSID == 'string' && data.EntriesPerSID) data.EntriesPerSID = JSON.parse(data.EntriesPerSID);

    let bulkActions = [];

    if(data.EntriesPerSID) {
      Object.keys(data.EntriesPerSID).forEach(SID => {
        bulkActions.push({
          updateOne: {
            "filter": {SID},
            "update": {
              $set: {
                "TimeHistory.$[n].Status": data.Status,
                "TimeHistory.$[n].Admin": data.Admin,
                "TimeHistory.$[n].AdminComment": data.AdminComment,
                "TimeHistory.$[n].DateModified": new Date(),
              }
            },
            "arrayFilters": [
              {
                "n.DateCreated": {$in: data.EntriesPerSID[SID].map(ds => new Date(ds-0))}
              }
            ],
          }
        })
      })
    }

    let bulkResponse = await Wrapper.DB.bulkWrite(bulkActions);

    Promise.all(Object.keys(data.EntriesPerSID).map(SID => {
      return Wrapper.fn.invalidateBySID({SID});
    })).then(results => {
      let success = bulkResponse.modifiedCount == bulkActions.length;
      let message = success ? undefined : `Could only update ${bulkResponse.modifiedCount} members out of ${bulkActions.length}`;
      return resolve({success, message});
    })

    // each SID in entries
  }))

  AddRoute('saveDetails', data => new Promise(resolve => {
    
  }))

  AddRoute('saveBitcoin', data => new Promise(async resolve => {
    if(!data || !data.SID) return resolve({success: false, message: 'No SID provided'});
    if(!data.BitcoinAddress) return resolve({success: false, message: 'No Bitcoin Address provided'});
    await Wrapper.DB.updateOne({SID: data.SID}, {
      $set: {
        BitcoinAddress: data.BitcoinAddress
      }
    })
    return resolve({success: true});
  }))

  AddRoute('withdrawBitcoin', data => new Promise(async resolve => {
    if(!data || !data.SID) return resolve({success: false, message: 'No SID provided'});
    if(!data.Amount) return resolve({success: false, message: 'No Amount provided'});
    if(!data.ConversionRate) return resolve({success: false, message: 'No ConversionRate provided'});

    let deductFees = false;

    let TotalBTC = Math.floor( (deductFees ? ( data.Amount * .97 - 10 ) : data.Amount) * data.ConversionRate * 1000000 ) / 1000000;

    let user = await Wrapper.DB.findOne({SID: data.SID});
    if(!user) return resolve({success: false, message: 'A user with the specified SID was not found'});
    if(user.Bank?.MonetizedWallet?.Balance < data.Amount) return resolve({success: false, message: `You don't have enough eWallet funds for this withdrawal`});
    
    let History = user.Bank?.MonetizedWallet?.History || [];

    data.Amount = Number(data.Amount);
    if(data.Amount < 25) return resolve({success: false, message: 'Withdrawal minimum is 25'});
    if(data.Amount % 25 > 0) return resolve({success: false, message: 'Withdrawal needs to be in increments of 25'});
    if(!(History.find(s => s.Notes.indexOf("Bitcoin Transaction") > -1 && s.WalletAddress == user.BitcoinAddress && s.Status == "paid")) && data.Amount > 25) return resolve({success: false, message: `Your first withdrawal with a new Bitcoin address needs to be 25`});
    if(History.find(s => s.Notes.indexOf("Bitcoin Transaction") > -1 && s.Status == "pending")) return resolve({success: false, message: `You currently have a pending withdrawal`});

    // let RankIndex = Number(data.RankIndex || user.RankIndex);
    // let CurrentWeekMonetizedSpending = [0, ...MonetizedSpending.filter(s => moment(s.Date).diff(moment().startOf('week')) > 0).map(i => i.Amount)].reduce((a,b) => a+b);
    // let WeeklyLimit = [0,100,100,300,300,300,1300,1300,1300,2500,2500,2500,6300,6300,6300,6300,6300,25000][RankIndex];
    // if(CurrentWeekMonetizedSpending + data.Amount > WeeklyLimit) return resolve({success: false, message: `You cannot exceed your weekly monetized limit of ${WeeklyLimit}`});

    if(!user.BitcoinAddress) return resolve({success: false, message: `You don't have a bitcoin address saved.`});
    if(!(/^[13][a-km-zA-HJ-NP-Z1-9]{25,34}$/.exec(user.BitcoinAddress))) return resolve({success: false, message: `Your bitcoin address is invalid.`});

    let eventDate = new Date();
    await Wrapper.DB.updateOne({SID: user.SID}, {
      $inc: {
        "Bank.MonetizedWallet.Balance": -data.Amount
      },
      $push: {
        "Bank.MonetizedWallet.History": {
          Amount: -data.Amount,
          Reason: `Bitcoin Withdrawal (${TotalBTC} BTC)`,
          Date: eventDate,
          Approved: true,
          DateApproved: eventDate,
          Notes: [`Withdrawal of ${TotalBTC} to wallet ${user.BitcoinAddress}`, `Bitcoin Transaction`],
          Status: "pending",
          TotalUSD: data.Amount,
          TotalBTC: data.Amount * data.ConversionRate,
          WalletAddress: user.BitcoinAddress,
        }
      }
    })
    return resolve({success: true});
  }))

  AddRoute('logMonetizedTokenGeneration', data => new Promise(async resolve => {
    if(!data || !data.SID) return resolve({success: false, message: 'No SID provided'});
    if(!data.MonetizedBalance) return resolve({success: false, message: 'No MonetizedBalance provided'});

    let user = await Wrapper.DB.findOne({SID: data.SID});
    if(!user) return resolve({success: false, message: 'A user with the specified SID was not found'});

    let eventDate = new Date();
    await Wrapper.DB.updateOne({SID: user.SID}, {
      $inc: {
        "Bank.MonetizedWallet.Balance": -data.Amount * 99,
      },
      $push: {
        "Bank.MonetizedWallet.History": {
          Amount: -data.Amount * 99,
          Reason: `Generated Elamant Tokens`,
          Date: eventDate,
          Approved: true,
          DateApproved: eventDate,
          Notes: [`Generation of ${data.Amount} Elamant Tokens`, `Elamant Token`],
        },
      }
    })

    return resolve({success: true});
  }))

  AddRoute('monetize', data => new Promise(async resolve => {
    if(!data || !data.SID) return resolve({success: false, message: 'No SID provided'});
    if(!data.AdminSID) return resolve({success: false, message: 'No AdminSID provided'});

    let user = await Wrapper.DB.findOne({SID: data.SID});
    if(!user) return resolve({success: false, message: 'A user with the specified SID was not found'});

    let monetizedDate = new Date();
    await Wrapper.DB.updateOne({SID: user.SID}, {
      $set: {
        Monetized: true,
        DateLastMonetized: monetizedDate,
        MonetizedByAdmin: data.AdminSID
      },
      $push: {
        DatesMonetized: monetizedDate
      }
    })
    await MDB.collection('genealogy').updateOne({SID: user.SID} ,{
      $set: {
        Monetized: true,
        DateLastMonetized: monetizedDate,
      }
    });

    // if(!user.Monetized) {  Make All Renewals Count
      let parent = await Wrapper.DB.findOne({SID: user.ParentID});
      if(parent && parent.Monetized) {
        let countedMembers = (parent.CountedMonetizedMembers || []);
        let amountToCredit = ((countedMembers.indexOf(user.SID) > -1) ? 25 : (countedMembers.length < 3 ? 25 : 50));
        await Wrapper.DB.updateOne({SID: parent.SID}, {
          $set: {
            "Bank.MonetizedWallet.Balance": (parent.Bank.MonetizedWallet || {Balance: 0}).Balance + amountToCredit,
          },
          $push: {
            CountedMonetizedMembers: user.SID,
            "Bank.MonetizedWallet.History": {
              Amount: amountToCredit,
              Reason: `Sponsor Monetization Bonus (${user.SID})`,
              Date: new Date(),
              Approved: true,
              DateApproved: new Date(),
              Notes: [`Sponsor Monetization Bonus`]
            }
          }
        })

        await Wrapper.fn.invalidateBySID({SID: parent.SID});
      }
    // }

    Wrapper.fn.invalidateBySID({SID: user.SID});

    return resolve({success: true});
  }))

  AddRoute('getMonetizedWalletData', data => new Promise(async resolve => {
    let walletData = [...await Wrapper.DB.aggregate([
      {$match: {Active: true, Monetized: true, "Bank.MonetizedWallet.Balance": {$gte: 25}}},
      {$project: {WalletBalance: "$Bank.MonetizedWallet.Balance", MonetizedSpending: "$Bank.MonetizedWallet.MonetizedSpending", MonetizationScore: 1}},
      {$group: {
        _id: null,
        count: {$sum: 1},
        available: {$sum: "$WalletBalance"}
      }}
    ]).toArray(), {}][0];
    return resolve({success: !!walletData.count, ...walletData});
  }))

  AddRoute('cancelWithdrawBitcoin', data => new Promise(async resolve => {
    if(!data || !data.SID) return resolve({success: false, message: 'No SID provided'});
    if(!data.Date) return resolve({success: false, message: 'No Date provided'});
    if(!data.TotalBTC) return resolve({success: false, message: 'No TotalBTC provided'});

    let user = await Wrapper.DB.findOne({SID: data.SID});
    if(!user) return resolve({success: false, message: 'A user with the specified SID was not found'});

    let itemToRemove = (user.Bank.MonetizedWallet?.History ?? []).find(e => (e.Date - new Date(data.Date-0) == 0) && (e.Notes.indexOf('Bitcoin Transaction') > -1));
    if(!itemToRemove) return resolve({success: false, message: 'Withdrawal marked for cancellation not found'});

    let amountToRefund = -itemToRemove.Amount;

    itemToRemove.Status = "refund";
    itemToRemove.Amount = 0;
    itemToRemove.Notes.push('Cancelled');

    await Wrapper.DB.updateOne({SID: user.SID}, {
      $inc: {
        "Bank.MonetizedWallet.Balance": amountToRefund
      },
      $pull: {
        "Bank.MonetizedWallet.History": {
          Date: itemToRemove.Date,
          TotalBTC: itemToRemove.TotalBTC
        }
      }
    })
    await Wrapper.DB.updateOne({SID: user.SID}, {
      $push: {
        "Bank.MonetizedWallet.History": itemToRemove
      }
    })

    return resolve({success: true});
  }))

  AddRoute('refundWithdrawBitcoin', data => new Promise(async resolve => {
    if(!data || !data.SID) return resolve({success: false, message: 'No SID provided'});
    if(!data.Date) return resolve({success: false, message: 'No Date provided'});
    if(!data.TotalBTC) return resolve({success: false, message: 'No TotalBTC provided'});

    let user = await Wrapper.DB.findOne({SID: data.SID});
    if(!user) return resolve({success: false, message: 'A user with the specified SID was not found'});

    let itemToRemove = (user.Bank.MonetizedWallet?.History ?? []).find(e => (new Date(e.Date)-0) == (new Date(data.Date)-0) && e.Notes.indexOf('Bitcoin Transaction') > -1);
    if(!itemToRemove) return resolve({success: false, message: 'Withdrawal marked for cancellation not found'});

    let amountToRefund = -itemToRemove.Amount;

    itemToRemove.Status = "refund";
    itemToRemove.Amount = 0;
    itemToRemove.Notes.push('Refunded');

    await Wrapper.DB.updateOne({SID: user.SID}, {
      $inc: {
        "Bank.MonetizedWallet.Balance": amountToRefund
      },
      $pull: {
        "Bank.MonetizedWallet.History": {
          Date: itemToRemove.Date,
          TotalBTC: itemToRemove.TotalBTC
        }
      }
    })
    await Wrapper.DB.updateOne({SID: user.SID}, {
      $push: {
        "Bank.MonetizedWallet.History": itemToRemove
      }
    })

    return resolve({success: true});
  }))

  AddRoute('updateLocalData', data => new Promise(async resolve => {
    
  }))

  AddRoute('startRedemption', data => new Promise(async resolve => {
    if(!data || !data.SID) return resolve({success: false, message: 'No SID provided'});
    let user = await Wrapper.DB.findOne({SID: data.SID});
    if(!user) return resolve({success: false, message: 'A user with the specified SID was not found'});
    if(user.Bank.DataPoints.Redemption.EndDate > Date.now()) return resolve({success: false, message: `You are already in a redemption (until ${moment(user.Bank.DataPoints.Redemption.EndDate).format('D MMM YYYY')})`});
    
    let canAfford = false;
    if((data.AccumulatedReceiptPoints + data.RolloverPoints) >= 100 && data.CanRedeem100) canAfford = true;
    if((data.AccumulatedReceiptPoints + data.RolloverPoints) >= 200 && data.CanRedeem200) canAfford = true;
    if(!canAfford) return resolve({success: false, message: `You don't have enough Data Credits`});

    if(!data.HasRedeemed && data.Amount == 200) return resolve({success: false, message: `First redemption can't be for 200 Data Credits`});
    if(data.lastRedemptionTooRecent) return resolve({success: false, message: `You need to wait 30 days after your last redemption ended. (${moment(redemptionEnd).add(30, 'days').format('D MMM YYYY')})`})
    if(data.firstReceiptTooRecent) return resolve({success: false, message: `You need to wait until your first receipt is 30 days old. (${moment(firstReceiptDate).add(30, 'days').format('D MMM YYYY')})`})

    let totalPoints = 0;
    let accumulatedWeeklyPoints = {};
    let usedReceipts = [];
    (data.Receipts ?? []).filter(receipt => {
      if(receipt.UsedInRedemption || receipt.Status == 'redeemed') return false; // Receipt has been used
      if(moment(receipt.Date).startOf('day').diff(moment().add(-60, 'days')) < 0) return false; // Receipt is expired

      return true;
    }).sort((a,b) => b.Date - a.Date).forEach(receipt => {
      let week = moment(receipt.Date).startOf('week')-0;
      if(!accumulatedWeeklyPoints[week]) accumulatedWeeklyPoints[week] = 0;

      if(accumulatedWeeklyPoints[week] >= 40) return; // Ignore receipts that exceed the weekly limit
      
      if(totalPoints < data.Amount) {
        usedReceipts.push(receipt);
        accumulatedWeeklyPoints[week] = Math.min(40, accumulatedWeeklyPoints[week] + receipt.ConvertedAmount * 0.2);
        totalPoints += receipt.ConvertedAmount * 0.2;
      }
    })

    let accumulatedPoints =  data.AccumulatedReceiptPoints || [Object.values(accumulatedWeeklyPoints), 0].flat().reduce((a,b) => a+b) + user.Bank.DataCredits.RolloverPoints;

    if(accumulatedPoints < data.Amount) return resolve({success: false, message: `You don't have enough Data Credits`});

    let RedemptionDetails = {
      ID: GenerateSalt(8),
      ReceiptCount: usedReceipts.length,
      ReceiptIDs: usedReceipts.map(r => r._id),
      PointsPotential: data.Amount,
      PointsRollover: accumulatedPoints-data.Amount,
      PointsRedeemed: 0,
      Amount: accumulatedPoints,
      PaymentMethods: Array.from(new Set(usedReceipts.map(r => r.PaymentMethod))),
      Comments: [],
      Status: 'pending',
      DateCreated: new Date()
    }

    await MDB.collection('receipts').updateMany({_id: {$in: RedemptionDetails.ReceiptIDs.map(id => new ObjectID(id))}}, {
      $set: {
        UsedInRedemption: RedemptionDetails.ID,
        Status: 'redeemed'
      }
    })

    await Wrapper.DB.updateOne({SID: user.SID}, {
      $set: {
        "Bank.DataPoints.Redemption.Active": true,
        "Bank.DataPoints.Redemption.EndDate": moment().add(30, 'days').endOf('day').toDate(),
        "Bank.DataCredits.RolloverPoints": RedemptionDetails.PointsRollover
      },
      $push: {
        "Bank.DataPoints.Redemption.History": RedemptionDetails
      }
    })

    return resolve({success: true});
  }))

  AddRoute('setImagePath', data => new Promise(async resolve => {
    if(!data ||!data.SID) return resolve({success: false, message: 'No SID provided'});
    if (!data.ImagePath) return resolve({ success: false, message: 'No ImagePath provided' });
    
    /* Set the imageType on the bases of profile or banner image update request */
    if (data.imageType == 'banner') {
      await Wrapper.DB.updateOne({ SID: data.SID }, {
        $set: {
          HasImage: true,
          BannerPath: data.ImagePath,
        }
      })  
    } else {
      await Wrapper.DB.updateOne({ SID: data.SID }, {
        $set: {
          HasImage: true,
          ImagePath: data.ImagePath,
        }
      })
    }
    return resolve({success: true});
  }))

  AddRoute('addStore', data => new Promise(async resolve => {
    let {SID, PlaceID} = data || {};
    if(!SID) return resolve({success: false, message: 'No SID provided'});
    if(!PlaceID) return resolve({success: false, message: 'No PlaceID provided'});
    await Wrapper.DB.updateOne({SID}, {$push: {StoreIDs: PlaceID}})
    if(Wrapper.FetchedIDs[SID]) Wrapper.FetchedIDs[SID].StoreIDs.push(PlaceID);
    return resolve({success: true});
  }))

  AddRoute('removeStore', data => new Promise(async resolve => {
    let {SID, PlaceID} = data || {};
    if(!SID) return resolve({success: false, message: 'No SID provided'});
    if(!PlaceID) return resolve({success: false, message: 'No PlaceID provided'});
    await Wrapper.DB.updateOne({SID}, {$pull: {StoreIDs: PlaceID}})
    if(Wrapper.FetchedIDs[SID]) Wrapper.FetchedIDs[SID].StoreIDs = Wrapper.FetchedIDs[SID].StoreIDs.filter(id => id !== PlaceID);
    return resolve({success: true});
  }))

  AddRoute('updateShoppersProfile', data => new Promise(async resolve => {
    if(!data || !data.SID) return resolve({success: false, message: 'No SID provided'});
    let out = {
      "MarketResearch.LastUpdated": new Date(),
      "MarketResearch.Stats.AnnualCruises": data.AnnualCruises,
      "MarketResearch.Stats.AnnualGifts": data.AnnualGifts,
      "MarketResearch.Stats.AnnualGolfGames": data.AnnualGolfGames,
      "MarketResearch.Stats.AnnualHotelNights": data.AnnualHotelNights,
      "MarketResearch.Stats.AnnualHouseholdIncome": data.AnnualHouseholdIncome,
      "MarketResearch.Stats.AnnualOilChanges": data.AnnualOilChanges,
      "MarketResearch.Stats.AnnualThemeParkTickets": data.AnnualThemeParkTickets,
      "MarketResearch.Stats.MonthlyActivities": data.MonthlyActivities,
      "MarketResearch.Stats.MonthlyCarRentals": data.MonthlyCarRentals,
      "MarketResearch.Stats.MonthlyClothingPurchases": data.MonthlyClothingPurchases,
      "MarketResearch.Stats.MonthlyDryCleaners": data.MonthlyDryCleaners,
      "MarketResearch.Stats.MonthlyMovieTickets": data.MonthlyMovieTickets,
      "MarketResearch.Stats.MonthlyPizzas": data.MonthlyPizzas,
      "MarketResearch.Stats.MonthlySpaVisits": data.MonthlySpaVisits,
      "MarketResearch.Stats.PreferredPurchaseMethod": data.PreferredPurchaseMethod,
      "MarketResearch.Stats.TotalCellphones": data.TotalCellphones,
      "MarketResearch.Stats.TotalDependants": data.TotalDependants,
      "MarketResearch.Stats.WeeklyDinners": data.WeeklyDinners,
      "MarketResearch.Stats.WeeklyLunches": data.WeeklyLunches,
    }
    await Wrapper.DB.updateOne({SID: data.SID}, {$set: out});
    delete Wrapper.FetchedIDs[data.SID];
    return resolve({success: true});
  }))

  AddRoute('updateContactDetails', data => new Promise(async resolve => {
    if(!data || !data.SID) return resolve({success: false, message: 'No SID provided'});
    if(!data.AddressLine1) return resolve({success: false, message: 'No AddressLine1 provided'});
    if(!data.AddressLine2) return resolve({success: false, message: 'No AddressLine2 provided'});
    if(!data.Country) return resolve({success: false, message: 'No Country provided'});
    if(!data.State) return resolve({success: false, message: 'No State provided'});
    if(!data.City) return resolve({success: false, message: 'No City provided'});
    if(!data.ZipCode) return resolve({success: false, message: 'No ZipCode provided'});
    if(!data.Mobile) return resolve({success: false, message: 'No Mobile provided'});

    await Wrapper.DB.updateOne({SID: data.SID}, {$set: {
      "Address.Line1": data.AddressLine1,
      "Address.Line2": data.AddressLine2,
      "Address.Country": data.Country,
      "Address.State": data.State,
      "Address.City": data.City,
      "Address.ZipCode": data.ZipCode,
      Mobile: data.Mobile,
    }})

    return resolve({success: true});
  }))

  /* BOC update device token of loggedin user */
  AddRoute('updateDeviceToken', data => new Promise(async resolve => {
    
    if(!data || !data.SID) return resolve({success: false, message: 'No SID provided'});
    if(!data.DeviceToken) return resolve({success: false, message: 'No DeviceToken provided'});
    await Wrapper.DB.updateOne({SID: data.SID}, {$set: {
      DeviceToken: data.DeviceToken
    }})

    return resolve({success: true});
  }))
  /* EOC update device token of loggedin user */

  AddRoute('changePersonalInformation', data => new Promise(async resolve => {
    if(!data || !data.SID) return resolve({success: false, message: 'No SID provided'});
    if(!data.Name) return resolve({success: false, message: 'No Name provided'});
    if(!data.Surname) return resolve({success: false, message: 'No Surname provided'});
    if(!data.Email) return resolve({success: false, message: 'No Email provided'});
    if(!data.DateBirth) return resolve({success: false, message: 'No DateBirth provided'});
    if(!data.AdminSID) return resolve({success: false, message: 'No AdminSID provided'});

    await Wrapper.DB.updateOne({SID: data.SID}, {
      $set: {
        Name: data.Name,
        Surname: data.Surname,
        Email: data.Email,
        DateBirth: new Date(data.DateBirth),
      },
      $push: {
        AdminProfileEdits: {
          Date: new Date(),
          Admin: data.AdminSID
        }
      }
    })

    return resolve({success: true});
  }))

  AddRoute('updatePreOrderDetails', data => new Promise(async resolve => {
    if(!data || !data.SID) return resolve({success: false, message: 'No SID provided'});
    if(!data.AddressLine1) return resolve({success: false, message: 'No AddressLine1 provided'});
    if(!data.AddressLine2) return resolve({success: false, message: 'No AddressLine2 provided'});
    if(!data.City) return resolve({success: false, message: 'No City provided'});
    if(!data.Country) return resolve({success: false, message: 'No Country provided'});
    if(!data.DateBirth) return resolve({success: false, message: 'No DateBirth provided'});
    if(!data.Email) return resolve({success: false, message: 'No Email provided'});
    if(!data.Mobile) return resolve({success: false, message: 'No Mobile provided'});
    if(!data.Names) return resolve({success: false, message: 'No Names provided'});
    if(!data.State) return resolve({success: false, message: 'No State provided'});
    if(!data.ZipCode) return resolve({success: false, message: 'No ZipCode provided'});
    
    await Wrapper.DB.updateOne({SID: data.SID}, {$set: {
      "Bank.PayCard.PreOrder.Details": {
        AddressLine1: data.AddressLine1,
        AddressLine2: data.AddressLine2,
        City: data.City,
        Country: data.Country,
        DateBirth: data.DateBirth,
        Email: data.Email,
        Mobile: data.Mobile,
        Names: data.Names,
        State: data.State,
        ZipCode: data.ZipCode,
      }
    }})

    Wrapper.fn.invalidateBySID({SID: data.SID});

    return resolve({success: true});
  }))

  AddRoute('approvePayCard', data => new Promise(async resolve => {
    if(!data || !data.SID) return resolve({success: false, message: 'No SID provided'});
    if(!data.Admin) return resolve({success: false, message: 'No Admin provided'});
    
    
    await Wrapper.DB.updateOne({SID: data.SID}, {$set: {
      "Bank.PayCard.PreOrder.Status": {
        Paid: true,
        DateOrdered: new Date(),
        Status: 'paid',
        ApprovedByAdmin: data.Admin,
      }
    }})

    Wrapper.fn.invalidateBySID({SID: data.SID});

    return resolve({success: true});
  }))

  AddRoute('getCancelledPayCards', data => new Promise(async resolve => {
    let CancelledPayCardMembers = await Wrapper.DB.find({"Bank.PayCard.Cancelled": true, "Bank.PayCard.CancellationApproved": {$ne: true}}).toArray();
    let CancelledPayCards = CancelledPayCardMembers.map(member => {
      return {
        SID: member.SID,
        DateCancelled: member.Bank.PayCard.DateCancelled,
        Approved: member.Bank.PayCard.CancellationApproved,
      }
    })
    return resolve({success: true, items: CancelledPayCards})
  }))

  AddRoute('cancelPayCard', data => new Promise(async resolve => {
    if(!data || !data.SID) return resolve({success: false, message: 'No SID provided'});
    
    await Wrapper.DB.updateOne({SID: data.SID}, {$set: {
      "Bank.PayCard.Cancelled": true,
      "Bank.PayCard.DateCancelled": new Date(),
    }})

    Wrapper.fn.invalidateBySID({SID: data.SID});

    return resolve({success: true});
  }))

  AddRoute('approvePayCardCancellation', data => new Promise(async resolve => {
    if(!data || !data.SID) return resolve({success: false, message: 'No SID provided'});
    if(!data || !data.Method) return resolve({success: false, message: 'No Method provided'});
    if(!data || !data.AdminSID) return resolve({success: false, message: 'No AdminSID provided'});
    
    let user = await Wrapper.DB.findOne({SID: data.SID});
    if(!user) return resolve({success: false, message: 'User not found'});

    if(user.Bank.PayCard?.CancellationApproved) return resolve({success: false, message: "This member's PayCard Cancellation has already been approved"}); 
    if(!user.Bank.PayCard?.Cancelled) return resolve({success: false, message: "This member didn't request a PayCard Cancellation"});

    let payload = {success: true};

    let hasBonus = !!(user.Bank?.BonusWallet?.History ?? []).find(entry => entry.Amount == 50 && (/bonus/i).exec(entry.Reason))

    let changes = {
      $set: {
        "Bank.PayCard.CancellationApproved": true,
        "Bank.PayCard.DateCancellationApproved": new Date(),
        "Bank.PayCard.CancellationMethod": data.Method,
        "Bank.PayCard.CancellationAdmin": data.AdminSID
      }
    }

    if(hasBonus) {
      changes.$set = {...changes.$set, ...{
        "Bank.BonusWallet.Balance": (user.Bank?.BonusWallet?.Balance ?? 0) - 50
      }};
      changes.$push = {
        "Bank.BonusWallet.History": {
          Amount: -50,
          Reason: "PayCard Pre-Order Bonus Reversal (Order Cancelled)",
          Date: new Date(),
          Approved: true,
          DateApproved: new Date(),
          Notes: [`PayCard`, `Reversal`, `PayCard Cancellation by ${data.AdminSID}`],
        }
      }
    }

    switch(data.Method) {
      case 'token':
        if(!hasBonus) return resolve({success: false, message: 'This member only paid $29.95 for their PayCard order and cannot exchange it for a token'})
        payload.GenerateToken = true;
        break;
      case 'monetizedCredit':
        let refundAmount = hasBonus ? 99 : 30;
        changes.$set = {...changes.$set, ...{
          "Bank.MonetizedWallet.Balance": (user.Bank?.MonetizedWallet?.Balance ?? 0) + refundAmount
        }};
        changes.$push = {
          "Bank.MonetizedWallet.History": {
            Amount: refundAmount,
            Reason: "PayCard Pre-Order Bonus Reversal (Order Cancelled)",
            Date: new Date(),
            Approved: true,
            DateApproved: new Date(),
            Notes: [`PayCard`, `Reversal`, `PayCard Cancellation by ${data.AdminSID}`],
          }
        }
        break;
      case 'creditCard':
        break;
      case 'bankTransfer':
        break;
    }
    
    await Wrapper.DB.updateOne({SID: data.SID}, changes)

    Wrapper.fn.invalidateBySID({SID: data.SID});

    return resolve(payload);
  }))

  AddRoute('kycFilled', data => new Promise(async resolve => {
    if(!data || !data.SID) return resolve({success: false, message: 'No SID provided'});

    if(!data.Undo) {
      await Wrapper.DB.updateOne({SID: data.SID}, {$set: {
        HasFilledKYC: true,
        DateFilledKYC: new Date(),
      }})
    } else {
      await Wrapper.DB.updateOne({SID: data.SID}, {$unset: {
        HasFilledKYC: 1,
        DateFilledKYC: 1,
      }})
    }

    return resolve({success: true});
  }))

  AddRoute('approvePayCardByEmail', data => new Promise(async resolve => {
    if(!data || !data.Email) return resolve({success: false, message: 'No Email provided'});
    if(!data.Admin) return resolve({success: false, message: 'No Admin provided'});

    let user = await Wrapper.DB.findOne({Email: data.Email});
    if(!user) return resolve({success: false, message: 'User not found'});

    await Wrapper.DB.updateOne({Email: data.Email}, {$set: {
      "Bank.PayCard.PreOrder.Status": {
        Paid: true,
        DateOrdered: new Date(),
        Status: 'paid',
        ApprovedByAdmin: data.Admin,
      }
    }})

    Wrapper.fn.invalidateBySID({SID: data.SID});

    return resolve({success: true});
  }))

  AddRoute('updatePaymentMethods', data => new Promise(async resolve => {
    if(!data || !data.SID) return resolve({success: false, message: 'No SID provided'});
    let user = await Wrapper.DB.findOne({SID: data.SID});
    if(!user) return resolve({success: false, message: 'User not found'});
    if(!data.PaymentMethod_1) return resolve({success: false, message: 'Payment Method 1 not found'});
    if(!data.PaymentMethod_2) return resolve({success: false, message: 'Payment Method 2 not found'});
    if(!data.PaymentMethod_3) return resolve({success: false, message: 'Payment Method 3 not found'});

    let PaymentMethods = [
      {
        Type: data.PaymentMethod_1,
        CardLastDigits: data.LastDigits_1,
        Status: user.PaymentMethods.find(pm => pm.Type == data.PaymentMethod_1 && (pm.Type == 'Cash' || pm.CardLastDigits == data.LastDigits_1)) ? 'used' : 'active',
        DateChanged: new Date(),
      },
      {
        Type: data.PaymentMethod_2,
        CardLastDigits: data.LastDigits_2,
        Status: user.PaymentMethods.find(pm => pm.Type == data.PaymentMethod_2 && (pm.Type == 'Cash' || pm.CardLastDigits == data.LastDigits_2)) ? 'used' : 'active',
        DateChanged: new Date(),
      },
      {
        Type: data.PaymentMethod_3,
        CardLastDigits: data.LastDigits_3,
        Status: user.PaymentMethods.find(pm => pm.Type == data.PaymentMethod_3 && (pm.Type == 'Cash' || pm.CardLastDigits == data.LastDigits_3)) ? 'used' : 'active',
        DateChanged: new Date(),
      },
    ]

    await Wrapper.DB.updateOne({SID: user.SID}, {
      $set: {
        PaymentMethods,
        "MarketResearch.DefaultCurrency": data.DefaultCurrency
      }
    })

    return resolve({success: true});
  }))

  AddRoute('Create', data => new Promise(async resolve => {
    await Wrapper.DB.insertOne(data)
  }))

  // AddRoute('SetStripeCustomerID', data => new Promise(async resolve => {
  //   if(!data || !data.SID) return resolve({success: false, message: 'No SID provided'});
  //   if(!data.CustomerID) return resolve({success: false, message: 'No CustomerID provided'});
    
  //   await Wrapper.DB.updateOne({SID: data.SID}, {
  //     $set: {
  //       "Stripe.CustomerID": data.CustomerID
  //     }
  //   });

  //   return resolve({success: true});
  // }))

  // AddRoute('UpdateStripeCustomerID', data => new Promise(async resolve => {
  //   if(!data || !data.SID) return resolve({success: false, message: 'No SID provided'});
  //   if(!data.CustomerID) return resolve({success: false, message: 'No CustomerID provided'});
  //   if(Wrapper.FetchedIDs && Wrapper.FetchedIDs[data.SID]) {
  //     if(!Wrapper.FetchedIDs[data.SID].Stripe) Wrapper.FetchedIDs[data.SID].Stripe = {};
  //     Wrapper.FetchedIDs[data.SID].Stripe.CustomerID = data.CustomerID;
  //   }
  // }))

  let EnrollmentsQueue = [];
  let EnrollmentsQueueProcessing = false;

  const ProcessEnrollmentsQueue = async () => {
    if(EnrollmentsQueue.length <= 0) {
      EnrollmentsQueueProcessing = false;
      return;
    };

    let nextEnrollment = EnrollmentsQueue.splice(0, 1)[0];
    let newUser = nextEnrollment.User;
    
    let existingUsername = await Wrapper.DB.findOne({Username: {$regex: new RegExp(`^${newUser.Username}$`, 'i')}});
    if(existingUsername) {
      nextEnrollment.resolve({success: false, message: 'That username is already taken'});
      return ProcessEnrollmentsQueue();
    }

    let existingEmail = await Wrapper.DB.findOne({Email: newUser.Email});
    if(existingEmail) {
      nextEnrollment.resolve({success: false, message: 'That email address is already taken'});
      return ProcessEnrollmentsQueue();
    }

    let existingParent = await Wrapper.DB.findOne({SID: newUser.ParentID});
    if(!existingParent) {
      nextEnrollment.resolve({success: false, message: 'The specified sponsor could not be found'});
      return ProcessEnrollmentsQueue();
    }

    (await Wrapper.DB.find({SID: {$regex: /^\d+$/}}).sort({SID:-1}).limit(1).collation({locale: "en_US", numericOrdering: true})).forEach(async c => {
      newUser.SID = (c.SID-0+1).toString();
      await Wrapper.DB.insertOne(newUser);
      await MDB.collection('genealogy').insertOne({
        SID: newUser.SID,
        ParentID: newUser.ParentID,
        RegistrationType: newUser.RegistrationType,
        Status: newUser.Status,
        DateExpiring: newUser.DateExpiring,
        Type: newUser.Type,
        Active: newUser.Active,
        Username: newUser.Username,
      });
      nextEnrollment.resolve({success: true, SID: newUser.SID})

      ProcessEnrollmentsQueue();
    })
  }

  AddRoute('enroll', data => new Promise(async resolve => {
    let {ParentID, RegistrationType, Country, DateBirth, Email, MobileExtension, MobileNumber, Name, Surname, Password, PasswordConfirm, Username} = data;
    if(!ParentID) return resolve({success: false, message: 'No ParentID provided'});
    if(!RegistrationType) return resolve({success: false, message: 'No RegistrationType provided'});
    if(!Country) return resolve({success: false, message: 'No Country provided'});
    if(!DateBirth) return resolve({success: false, message: 'No DateBirth provided'});
    if(!Email) return resolve({success: false, message: 'No Email provided'});
    if(!MobileExtension) return resolve({success: false, message: 'No MobileExtension provided'});
    if(!MobileNumber) return resolve({success: false, message: 'No MobileNumber provided'});
    if(!Name) return resolve({success: false, message: 'No Name provided'});
    if(!Surname) return resolve({success: false, message: 'No Surname provided'});
    if(!Password) return resolve({success: false, message: 'No Password provided'});
    if(!PasswordConfirm) return resolve({success: false, message: 'No PasswordConfirm provided'});
    if(!Username) return resolve({success: false, message: 'No Username provided'});

    if(!Password || (Password !== PasswordConfirm)) {
      return resolve({success: false, message: 'Please ensure the Password and Confirm Password fields match'});
    }
    if(!ParentID) {
      return resolve({success: false, message: 'No Parent ID provided'});
    }

    Username = Username.replace(/\W/g, '');

    let newUser = {
      ParentID,
      Username,
      Name,
      Surname,
      Email,
      Mobile: `+${MobileExtension} ${MobileNumber}`,
      RegistrationType,
      Address: {
        Line1:"",
        Line2:"",
        Country,
        State:null,
        City:"",
        ZipCode:"NA",
      },
      DateBirth,
      Role: "user",
      Status: "active",
      Active: true,
      Type: "user",
      Bank: {
        eWallet: {
          Balance: 0,
          History: [],
        },
        DataPoints: {
          Balance: 0,
          PendingPayments: [],
          History: [],
          Redemption: {
            Active: false,
            EndDate: new Date(0),
            History: []
          }
        },
        DataCredits: {
          RolloverPoints: 0,
          History: [],
        },
        MonetizedWallet: {
          Balance: 0,
          History: [],
        },
        BonusWallet: {
          Balance: 0,
          History: [],
        },
        RenewalPoints: 0,
        RenewalPointsMonetized: 0,
      },
      TimeHistory: [],
      PaymentMethods: [],
      BitcoinAddress: '',
      HasImage: false,
      MarketResearch: {
        LastUpdated: null,
        Stats: {
          AnnualCruises: 0,
          AnnualGifts: 0,
          AnnualGolfGames: 0,
          AnnualHotelNights: 0,
          AnnualHouseholdIncome: 0,
          AnnualOilChanges: 0,
          AnnualThemeParkTickets: 0,
          MonthlyActivities: 0,
          MonthlyCarRentals: 0,
          MonthlyClothingPurchases: 0,
          MonthlyDryCleaners: 0,
          MonthlyMovieTickets: 0,
          MonthlyPizzas: 0,
          MonthlySpaVisits: 0,
          PreferredPurchaseMethod: 0,
          TotalCellphones: 0,
          TotalDependants: 0,
          WeeklyDinners: 0,
          WeeklyLunches: 0,
        }
      },
      Suspended: false,
      Terminated: false,
      RankIndex: 0,
      DateJoined: moment().startOf('day').toDate(),
      DateExpiring: moment().startOf('day').add(30, 'days').toDate(),
      DatePasswordChanged: new Date(),
      DateLastRenewed: moment().startOf('day').toDate(),
    }
    
    newUser.PasswordSalt = GenerateSalt();
    newUser.PasswordHash = Encrypt(Password, newUser.PasswordSalt);
    newUser.PasswordEngine = 1;

    EnrollmentsQueue.push({
      User: newUser,
      resolve
    })

    if(EnrollmentsQueueProcessing) return;
    EnrollmentsQueueProcessing = true;
    ProcessEnrollmentsQueue();
  }))

  const validateLatLng = (lat, lng) => {
    let pattern = new RegExp('^-?([1-8]?[1-9]|[1-9]0)\\.{1}\\d{1,6}');
    return typeof lat !== "string" && typeof lng !== "string" && pattern.test(lat) && pattern.test(lng);
  }
  AddRoute('getNearByMembers', data => new Promise(async resolve => {
    var stats = [];
    if (!data.request) return resolve({ success: false, message: 'No request type supplied' });
    if (!data.storeID) return resolve({ success: false, message: 'No storeID supplied' });
    if (!data.lat) return resolve({ success: false, message: 'No latitude supplied' });
    if (!data.lng) return resolve({ success: false, message: 'No longitude supplied' });
    if (!data.radius) return resolve({ success: false, message: 'No Radius supplied' });
    if (!validateLatLng(data.lat, data.lng)) return resolve({ success: false, message: 'Invalid coordinate values supplied' });

    await Wrapper.DB.find(
      {
        StoreIDs: data.storeID,
        Geolocation:
        {
          $near:
          {
            $geometry: { type: "Point", coordinates: [data.lat, data.lng] },
            $minDistance: 0,
            $maxDistance: data.radius
          }
        }
      }
    ).forEach(element => {
      stats.push(element.Geolocation);
    });

    return resolve({ success: true, data: stats });
  }))

  AddRoute('getCouponsByUserID', data => new Promise(async resolve => {
    var coupons = [];
    var couponsDetails = await Wrapper.DB.aggregate([
      {
        $match: {
          SID: data.SID
        }
      },
      {
        $lookup: {
          from: 'mapcoupons',
          localField: 'StoreIDs',
          foreignField: 'StoreID',
          as: 'CouponsList'
        }
      },
      {
        $project: {
          CouponsList: 1
        }
      }
    ]).toArray();
    
    couponsDetails[0]['CouponsList'].forEach(coupon => {
      var CurrentDate = moment().format('YYYY-MM-DD');
      var DateStart = moment(coupon.DateStart).format('YYYY-MM-DD');
      var DateExpired = moment(coupon.DateExpired).format('YYYY-MM-DD');

      if (coupon.Status == 1 && (CurrentDate >= DateStart && CurrentDate <= DateExpired)) {
        coupons.push(coupon);
      }
    });
    return resolve({ success: true, data: coupons || [] });
  }))

  AddRoute('getBitcoinWithdrawalData', data => new Promise(async resolve => {
    if(!data || !data.Date) return resolve({success: false, message: 'No date provided'});
    let dateStart = moment(data.Date).startOf('day').toDate();
    let dateEnd = moment(data.Date).endOf('day').toDate();

    let statusCheck = data.Status ? {
      '$eq': [
        '$$entry.Status', data.Status
      ]
    } : {};

    let withdrawalItems = await Wrapper.DB.aggregate(
      [
        {
          '$match': {
            'Active': true, 
            'Monetized': true, 
            'Suspended': {
              '$ne': true
            }, 
            'Terminated': {
              '$ne': true
            }, 
            'HasFilledKYC': true
          }
        }, {
          '$project': {
            'SID': 1, 
            'OldPayments': {
              '$filter': {
                'input': '$Bank.MonetizedWallet.History', 
                'as': 'entry', 
                'cond': {
                  '$and': [
                    {
                      '$in': [
                        'Bitcoin Transaction', '$$entry.Notes'
                      ]
                    }, {
                      '$lte': [
                        '$$entry.Date', dateStart
                      ]
                    }
                  ]
                }
              }
            }, 
            'PendingPayment': {
              '$last': {
                '$filter': {
                  'input': '$Bank.MonetizedWallet.History', 
                  'as': 'entry', 
                  'cond': {
                    '$and': [
                      {
                        '$in': [
                          'Bitcoin Transaction', '$$entry.Notes'
                        ]
                      }, {
                        '$gte': [
                          '$$entry.Date', dateStart
                        ]
                      }, {
                        '$lte': [
                          '$$entry.Date', dateEnd
                        ]
                      }, statusCheck
                    ]
                  }
                }
              }
            }
          }
        }, {
          '$match': {
            'PendingPayment': {
              '$ne': null
            }
          }
        }, {
          '$project': {
            '_id': 0, 
            'BTC': '$PendingPayment.TotalBTC',
            'USD': '$PendingPayment.TotalUSD',
            'WalletAddress': '$PendingPayment.WalletAddress',
          }
        }
      ]).toArray();

      let totalItems = withdrawalItems.length;
      let totalBTC = [...withdrawalItems.map(i => i.BTC), 0].reduce((a,b) => a+b);
      let totalUSD = [...withdrawalItems.map(i => i.USD), 0].reduce((a,b) => a+b);
      withdrawalItems = withdrawalItems.map(item => `${item.WalletAddress},${item.BTC}`);

      return resolve({success: true, items: withdrawalItems, count: totalItems, btc: totalBTC, usd: totalUSD});
  }))

  AddRoute('removeDuplicateBTCEntries', data => new Promise(async resolve => {
    if(!data.Date) return resolve({success: false, message: 'No Date provided'});

    let StartDate = moment(data.Date).startOf('day').toDate();
    var EndDate = moment(data.Date).add(1, 'day').startOf('day').toDate();
    
    let duplicateAddresses = await Wrapper.DB.aggregate([
      {
        '$match': {
          'Active': true, 
          'Monetized': true, 
          'Suspended': {
            '$ne': true
          }, 
          'Terminated': {
            '$ne': true
          }, 
          'HasFilledKYC': true
        }
      }, {
        '$project': {
          'SID': 1, 
          'OldPayments': {
            '$filter': {
              'input': '$Bank.MonetizedWallet.History', 
              'as': 'entry', 
              'cond': {
                '$and': [
                  {
                    '$in': [
                      'Bitcoin Transaction', '$$entry.Notes'
                    ]
                  }, {
                    '$lt': [
                      '$$entry.Date', StartDate
                    ]
                  }
                ]
              }
            }
          }, 
          'PendingPayment': {
            '$last': {
              '$filter': {
                'input': '$Bank.MonetizedWallet.History', 
                'as': 'entry', 
                'cond': {
                  '$and': [
                    {
                      '$in': [
                        'Bitcoin Transaction', '$$entry.Notes'
                      ]
                    }, {
                      '$gte': [
                        '$$entry.Date', StartDate
                      ]
                    }, {
                      '$lt': [
                        '$$entry.Date', EndDate
                      ]
                    }, {
                      $eq: ['$$entry.Status', 'pending']
                    }
                  ]
                }
              }
            }
          }
        }
      }, {
        '$match': {
          'PendingPayment': {
            '$ne': null
          }
        }
      }, {
        '$addFields': {
          'WalletAddress': '$PendingPayment.WalletAddress'
        }
      }, {
        '$group': {
          '_id': '$WalletAddress', 
          'count': {
            '$sum': 1
          }, 
          'BTC': {
            '$sum': '$PendingPayment.TotalBTC'
          }, 
          'USD': {
            '$sum': '$PendingPayment.TotalUSD'
          }
        }
      }, {
        '$match': {
          'count': {
            '$gt': 1
          }
        }
      }, {
        '$project': {
          '_id': 0,
          'WalletAddress': '$_id'
        }
      }
    ]).toArray();
  
    duplicateAddresses = duplicateAddresses.map(c => c.WalletAddress);
  
    let duplicateAddresses2 = [...duplicateAddresses];
  
    let duplicateUsers = [];
    let bulkTasks = [];
  
    let getNextAddressUsers = async () => {
      if(!duplicateAddresses.length) return credit();
      let WalletAddress = duplicateAddresses.pop();
      let users = (await Wrapper.DB.find({"Bank.MonetizedWallet.History": {"$elemMatch": {
        Status: 'pending',
        WalletAddress
      }}}).toArray()).map(u => {
        let Bank = {
          MonetizedWallet: {
            Balance: u.Bank?.MonetizedWallet?.Balance ?? 0,
            History: (u.Bank?.MonetizedWallet?.History ?? []).filter(e => e.WalletAddress == WalletAddress && e.Date >= StartDate && e.Date < EndDate)
          }
        }
        let totalReturnAmount = [0, Bank.MonetizedWallet.History.map(e => -e.Amount)].reduce((a,b) => (a-0)+(b-0));
        return {
          SID: u.SID,
          Bank,
          totalReturnAmount
        }
      });
      duplicateUsers = [...duplicateUsers, ...users];
      getNextAddressUsers();
    }
  
    let credit = async () => {
      bulkTasks = duplicateUsers.map(user => {
        return {
          updateOne: {
            filter: {SID: user.SID},
            update: {
              $set: {
                "Bank.MonetizedWallet.Balance": user.Bank.MonetizedWallet.Balance + user.totalReturnAmount
              }
            }
          }
        }
      })
  
      if(bulkTasks.length) await Wrapper.DB.bulkWrite(bulkTasks);
  
      return finish();
    }
  
    let finish = async () => {
      await Wrapper.DB.updateMany(
        {"Bank.MonetizedWallet.History": {$elemMatch: {WalletAddress: {$in: duplicateAddresses2}, Notes: "Bitcoin Transaction", Date: {$gte: StartDate, $lt: EndDate}, Status: "pending"}}},
        
        { $set: { "Bank.MonetizedWallet.History.$[n].Status": "invalid", "Bank.MonetizedWallet.History.$[n].Amount": 0, "Bank.MonetizedWallet.History.$[n].Note": "because the Bitcoin address is being used on another member account" } },
        
        { arrayFilters: [  { "n.WalletAddress": {$in: duplicateAddresses2}, "n.Date": {$gte: StartDate, $lt: EndDate}, "n.Status": "pending" } ], multi: true}
      )
  
      await Wrapper.fn.invalidateManyBySID({SIDs: duplicateAddresses2});
  
      return resolve({success: true});
    }

    getNextAddressUsers();
  }))

  AddRoute('removeInvalidBTCAddresses', data => new Promise(async resolve => {
    if(!data || !data.Date) return resolve({success: false, message: 'No Date provided'});
    if(!data.InvalidAddresses) return resolve({success: false, message: 'No InvalidAddresses provided'});

    let StartDate = moment(data.Date).startOf('day').toDate();
    var EndDate = moment(data.Date).add(1, 'day').startOf('day').toDate();

    let invalidAddresses = data.InvalidAddresses.split(',');
  
    let invalidAddresses2 = [...invalidAddresses];
  
    let duplicateUsers = [];
    let bulkTasks = [];
  
    let getNextAddressUsers = async () => {
      if(!invalidAddresses.length) return credit();
      let WalletAddress = invalidAddresses.pop();
      let users = (await Wrapper.DB.find({"Bank.MonetizedWallet.History": {"$elemMatch": {
        WalletAddress
      }}}).toArray()).map(u => {
        let Bank = {
          MonetizedWallet: {
            Balance: u.Bank?.MonetizedWallet?.Balance ?? 0,
            History: (u.Bank?.MonetizedWallet?.History ?? []).filter(e => e.WalletAddress == WalletAddress && e.Date >= StartDate && e.Date < EndDate)
          }
        }
        let totalReturnAmount = [0, Bank.MonetizedWallet.History.map(e => -e.Amount)].map(i => !!i).reduce((a,b) => (a-0)+(b-0));
        return {
          SID: u.SID,
          Bank,
          totalReturnAmount
        }
      });
      duplicateUsers = [...duplicateUsers, ...users];
      getNextAddressUsers();
    }
  
    let credit = async () => {
      bulkTasks = duplicateUsers.map(user => {
        return {
          updateOne: {
            filter: {SID: user.SID},
            update: {
              $set: {
                "Bank.MonetizedWallet.Balance": user.Bank.MonetizedWallet.Balance + user.totalReturnAmount
              }
            }
          }
        }
      })
  
      if(bulkTasks.length) await Wrapper.DB.bulkWrite(bulkTasks);
  
      return finish();
    }
  4
    let finish = async () => {
      await Wrapper.DB.updateMany(
        {"Bank.MonetizedWallet.History": {$elemMatch: {WalletAddress: {$in: invalidAddresses2}, Notes: "Bitcoin Transaction", Date: {$gte: StartDate, $lt: EndDate}, Status: "pending"}}},
        
        { $set: { "Bank.MonetizedWallet.History.$[n].Status": "invalid", "Bank.MonetizedWallet.History.$[n].Amount": 0 } },
        
        { arrayFilters: [  { "n.WalletAddress": {$in: invalidAddresses2}, "n.Date": {$gte: StartDate, $lt: EndDate}, "n.Status": "pending" } ], multi: true}
      )

      await Wrapper.fn.invalidateManyBySID({SIDs: invalidAddresses2});
  
      return resolve({success: true});
    }

    getNextAddressUsers();
  }))

  AddRoute('markBTCPaid', data => new Promise(async resolve => {
    if(!data || !data.Date) return resolve({success: false, message: 'No Date provided'});

    let StartDate = moment(data.Date).startOf('day').toDate();
    var EndDate = moment(data.Date).add(1, 'day').startOf('day').toDate();

    await Wrapper.DB.updateMany(
      {"Bank.MonetizedWallet.History": {$elemMatch: {Notes: "Bitcoin Transaction", Date: {$gte: StartDate, $lt: EndDate}, Status: "pending"}}},
      
      { $set: { "Bank.MonetizedWallet.History.$[n].Status": "paid" } },
      
      { arrayFilters: [  { "n.Date": {$gte: StartDate, $lt: EndDate}, "n.Status": "pending" } ], multi: true}
    )

    return resolve({success: true});
  }))


  AddRoute('getExpectedIncome', data => new Promise(async resolve => {
    var Users = {};
    let StartDate = moment().startOf('day').toDate();
    var EndDate = moment().add(3, 'days').startOf('day').toDate();

    await Wrapper.DB.find({ "Role": "user", "DateExpiring": { $gte: new Date(StartDate), $lte: new Date(EndDate) } }, { "sort": ['DateExpiring', 'asc'], fields: { DateExpiring: 1 } }).forEach(element => {
      if (!Users[moment(element.DateExpiring).format('D MMM')]) {
        Users[moment(element.DateExpiring).format('D MMM')] = 99;
      } else {
        Users[moment(element.DateExpiring).format('D MMM')] += 99;
      }        
    });   
    return resolve({ success: true, data: Users });
  }))
}