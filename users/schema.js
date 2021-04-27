const moment = require('moment');

module.exports = function(Wrapper) {
  function User(data = {}) {
    this.SID = data.SID;
    this.ParentID = data.ParentID;
    this.Username = data.Username;
    this.Name = data.Name;
    this.Surname = data.Surname;
    this.Email = data.Email;
    this.Bank = data.Bank;

    if(!this.Bank) this.Bank = {};

    if(this.Bank?.DataPoints?.Redemption) {
      if(this.Bank.DataPoints.Redemption.History.length) {
        let lastRedemption = this.Bank.DataPoints.Redemption.History.sort((a,b) => (new Date(b.DateCreated)-0) - (new Date(a.DateCreated)-0))[0].DateCreated;
        this.Bank.DataPoints.Redemption.Active = moment(lastRedemption).endOf('day').add(30, 'days').diff() > 0;
        this.Bank.DataPoints.Redemption.EndDate = moment(lastRedemption).endOf('day').add(30, 'days').toDate();
      } else {
        this.Bank.DataPoints.Redemption.Active = false
      }
    }

    if(!this.Bank.MonetizedWallet) {
      this.Bank.MonetizedWallet = {
        Balance: 0,
        History: [],
      }
    }

    if(!this.Bank.BonusWallet) {
      this.Bank.BonusWallet = {
        Balance: 0,
        History: [],
      }
    }

    if(!this.Bank.RenewalPointsMonetized) {
      this.Bank.RenewalPointsMonetized = 0;
    }

    if(!this.Bank.RenewalHistoryMonetized) {
      this.Bank.RenewalHistoryMonetized = [];
    }

    this.TimeHistory = data.TimeHistory ?? [];
    this.PaymentMethods = data.PaymentMethods ?? [];
    this.PasswordHash = data.PasswordHash;
    this.PasswordSalt = data.PasswordSalt;
    this.PasswordEngine = data.PasswordEngine;
    this.DateJoined = data.DateJoined;
    this.DateExpiring = data.DateExpiring;
    this.DatePasswordChanged = data.DatePasswordChanged;
    this.DateLastLogin = data.DateLastLogin;
    this.DateLastRenewed = data.DateLastRenewed;
    this.Package = data.Package;
    this.RegistrationMethod = data.RegistrationMethod;
    this.DefaultCurrency = data.DefaultCurrency;
    this.DefaultLanguage = data.DefaultLanguage;
    this.RegistrationType = data.RegistrationType;
    this.Role = data.Role;
    this.Status = data.Status;
    this.Type = data.Type;
    this.Active = data.Active;
    this.Address = data.Address;
    this.Mobile = data.Mobile;
    this.Landline = data.Landline;
    this.DateBirth = data.DateBirth;
    this.BitcoinAddress = data.BitcoinAddress;
    this.HasImage = data.HasImage;
    this.ImagePath = data.ImagePath;
    this.BannerPath = data.BannerPath;
    this.SocialMedia = data.SocialMedia;
    this.MarketResearch = data.MarketResearch || {Stats: {}};
    this.Suspended = data.Suspended ?? false;
    this.Terminated = (data.Terminated || (data.DateExpiring && moment(data.DateExpiring).add(60, 'days').endOf('day').diff() < 0)) ?? false;
    this.SuspensionHistory = data.SuspensionHistory ?? [];
    this.Terminated = data.Terminated ?? false;
    this.TerminationReason = data.TerminationReason;
    this.TerminationAdmin = data.TerminationAdmin;
    this.StoreIDs = data.StoreIDs ?? [];
    this.PendingStoreIDs = data.PendingStoreIDs ?? [];
    this.AcceptedTermsVersion = data.AcceptedTermsVersion ?? 1;
    this.RankIndex = data.RankIndex ?? 0;
    this.AdminExtensions = data.AdminExtensions ?? [];
    this.Monetized = data.Monetized || false;
    this.DateLastMonetized = data.DateLastMonetized || 0;
    this.HasFilledKYC = data.HasFilledKYC || false;
    this.DateFilledKYC = data.DateKYCFilled || 0;
    this.DeviceToken = data.DeviceToken || '';
    this.RankChanges = data.RankChanges || [];

    Wrapper.AllItems.push(this);
    Wrapper.FetchedIDs[this.SID] = this;
  }

  return User;
};