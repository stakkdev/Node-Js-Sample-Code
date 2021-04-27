const crypto = require('crypto'),
      ObjectID = require('mongodb').ObjectID;

module.exports = function(Wrapper) {
  const Prefetch = Wrapper.Prefetch;
  const Router = this;

  let AddRoute = require('../addRouteFactory')(Wrapper, Router);

  AddRoute('save', data => new Promise(async resolve => {
    if(!data.sessionId) return resolve({success: false, message: 'No SessionId provided'});
    if(!data.Email) return resolve({success: false, message: 'No Email provided'});
    if(!data.planId) return resolve({success: false, message: 'No PlanId provided'});
    if(!data.transactionId) return resolve({success: false, message: 'No transactionId provided'});
    
    let newTransactionID = new ObjectID();

    try {
      await Wrapper.DB.insertOne({
        _id: newTransactionID,
        Email: data.Email,
        PlanId: data.planId,
        TransactionId: data.transactionId,
        PaymentResponse: data.paymentResponse || {},
        DateCreated: new Date(),
        DateModified: new Date()
      })
    } catch(e) {
      return resolve({success: false, message: e.message});
    }

    return resolve({success: true});
  }))

  AddRoute('getTransactionsByDate', data => new Promise(resolve => {
    let search = {"Email": data.Email}

    if(data.date) {
      search.DateCreated =  new Date(data.date);
    }
    
    Wrapper.DB.find(search).toArray((err, transactions) => {
      if(err) return resolve({success: false, message: err.message});
      return resolve({success: true, items: transactions});
    })
  }))
}