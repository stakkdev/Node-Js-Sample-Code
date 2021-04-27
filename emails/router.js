const crypto  = require('crypto'),
      fs      = require('fs'),
      path    = require('path'),
      pug     = require('pug'),
      Mailer  = require('./_mailer');

const AdminMailer = new Mailer(require('nodemailer'), {
  pool: true,
  secure: true,
  name: `Elamant Platform`,
  user: process.env.MAIL_USER,
  pass: process.env.MAIL_PASS,
})

const Mailers = {
  Admin: AdminMailer
}

module.exports = function(Wrapper) {
  const Prefetch = Wrapper.Prefetch;
  const Router = this;

  let AddRoute = require('../addRouteFactory')(Wrapper, Router);

  AddRoute('sendEmail', data => new Promise(async resolve => {
    if(!data.To) return resolve({success: false, message: 'No To provided'});
    if(!data.Subject) return resolve({success: false, message: 'No Subject provided'});
    if(!data.Template) return resolve({success: false, message: 'No Template provided'});
    let p = path.join(__dirname, `/templates`, data.Template);
    if(!fs.existsSync(p))  return resolve({success: false, message: 'Template not found'});

    let mailer = Mailers[data.Account] || AdminMailer;


    let mailerSendData = {
      to: data.To,
      subject: data.Subject,
      body: pug.renderFile(path.join(__dirname, `/templates`, data.Template, 'template.pug'), data.Parameters)
    }

    if(data.Attachment == true){
      mailerSendData.attachments = data.AttachmentsPdfPath;
    }

    mailer.Send(mailerSendData).then(async (err, response) => {
      if(err) return resolve({success: false, message: err.message});

      await Wrapper.DB.insertOne({
        Recepient: data.To,
        Template: data.Template,
        Success: true,
        Data: {...data, To: undefined, Template: undefined},
        Date: new Date(),
      });
      return resolve({success: true});
    })
  }))

}
