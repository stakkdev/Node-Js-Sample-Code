var log = message => {console.log(`   mailer > ${message}`)};
var inlineCss = require('inline-css');

var previewService = {
  live: false,
  port: 64444,
  data: [],
  open: null,
  express: null,
  app: null,
  run: () => {
    log("Running mail preview service!");

    previewService.express = require('express');
    previewService.app = previewService.express();
    previewService.open = require('open');

    previewService.app.get('/:index', (req, res) => {
      if(!previewService.data[req.params.index])
        return res.send('No data!');

      res.send(previewService.data[req.params.index].html);
    })

    previewService.app.listen(previewService.port);
  },
  showPreview: () => {
    previewService.open(`http://localhost:${previewService.port}/${previewService.data.length-1}`);
  }
};

var debugMode;

class Mailer {
  constructor(nodemailer, config) {
    this.email = config.user;

    this.mailConfig = {
      service: 'gmail',
      auth: {
        user: config.user,
        pass: config.pass,
      }
    };

    debugMode = process.env.NODE_ENV !== 'production';

    if(debugMode) {
      previewService.run();
    }

    this.transporter = nodemailer.createTransport(this.mailConfig);
  }

  async Send(options) {
    return new Promise((resolve, reject) => {
      var mailOptions = {
        from: `Elamant <${this.mailConfig.auth.user}>`,
        to: options.to,
        bcc: process.env.BCC_EMAIL,
        subject: options.subject
      };

      if(options.attachments){
        mailOptions.attachments = options.attachments;
      }

      inlineCss(options.body, {url: ' '}).then(html => {
        mailOptions.html = html;

        if(debugMode) {
          previewService.data.push(mailOptions);
          previewService.showPreview();
          resolve(true);
        } else {
          this.transporter.sendMail(mailOptions, function(error, info){
            if (error) {
              console.log(error);
              return resolve({success: false, message: error});
            } else {
              console.log(info);
              return resolve({success: true});
            }
          });
          
        }
      });
    })
  }
}

module.exports = Mailer;
