module.exports = function(Wrapper) {
  const MapTransaction = function(data) {
    this._id = data._id;

    this.Email = data.Email;
    this.PlanId = data.PlanId;
    this.TransactionId = data.TransactionId;
    this.DateCreated = data.DateCreated;
    this.DateModified = data.DateModified;
    
    Wrapper.AllItems.push(this);
    Wrapper.FetchedIDs[this._id] = this;
  }
  return MapTransaction;
}
