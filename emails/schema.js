module.exports = function(Wrapper) {
  const Email = function(data) {
    this._id = data._id;

    this.Recepient = data.Recepient;
    this.Date = data.Date;
    this.Template = data.Template;
    this.Success = data.Success;
    this.Data = data.Data;

    Wrapper.AllItems.push(this);
    Wrapper.FetchedIDs[this._id] = this;
  }
  return Email;
}
