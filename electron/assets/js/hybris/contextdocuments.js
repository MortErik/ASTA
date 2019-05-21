/*
    Model is responsible for upload context documents
    initialize interface inputs: elements from <div id="hybris-panel-contextdocuments">
    output context documents at /ContextDocumentation
 */
window.Rigsarkiv = window.Rigsarkiv || {},
function (n) {
    Rigsarkiv.Hybris = Rigsarkiv.Hybris || {},
    function (n) {
        const { ipcRenderer } = require('electron');
        const {shell} = require('electron');
        const path = require('path');
        const os = require('os');
        const fs = require('fs');
        const domParser = require('xmldom');

        //private data memebers
        var settings = {
            structureCallback: null,
            okBtn: null,
            printBtn: null,
            outputErrorSpn: null,
            outputErrorText: null,
            uploadsTbl: null,
            outputEmptyFileTitle: null,
            outputEmptyFileText: null,
            overviewTab: null,
            outputOkInformationTitle: null,
            outputOkInformationText: null,
            spinner: null,
            spinnerClass: null,
            documents: [],
            logs: [],
            documentsPath: null,
            filePath: null,
            filePostfix: "{0}_ASTA_contextdocuments.html",
            templateFileName: "contextdocuments.html",
            scriptPath: "./assets/scripts/{0}",
            resourceWinPath: "resources\\{0}",
            contextDocumentationFolder: "ContextDocumentation",
            docCollectionFolderName: "docCollection1"
        }

         //output system error messages
         var HandleError = function(err) {
            console.log(`Error: ${err}`);
            var msg = ""
            if (err.code === "ENOENT") {
                msg = "Der er opstået en fejl i dannelsen af afleveringspakken. Genstart venligst programmet.";
            }
            else {
                msg = err.message
            }
            settings.outputErrorSpn.hidden = false;
            settings.outputErrorSpn.innerHTML = settings.outputErrorText.format(msg);       
        }

        //reset status & input fields
        var Reset = function () {
            settings.outputErrorSpn.hidden = true;
            $("#{0} tr:not(:first-child)".format(settings.uploadsTbl.id)).remove();
            settings.documents = [];
        }

        //get JSON upload document by id
        var GetDocument = function (id) {
            var result = null;
            settings.documents.forEach(upload => {
                if(upload.id === id) {
                    result = upload;
                }
            });
            return result;
        }

        // Render Documents control
        var RenderDocuments = function(data) {
            if(data != null && data.toString() != null && data.toString() !== "") {
                var doc = new domParser.DOMParser().parseFromString(data.toString());
                for(var i = 0; i < doc.documentElement.childNodes.length;i++) {
                    var node = doc.documentElement.childNodes[i];
                    if(node.nodeName === "document" && node.childNodes != null) {
                        var upload = {"id":"", "title":"", "path":""};
                        for(var j = 0; j < node.childNodes.length;j++) {
                            if(node.childNodes[j].nodeName === "documentID") {
                                upload.id = node.childNodes[j].firstChild.data;
                            }
                            if(node.childNodes[j].nodeName === "documentTitle") {
                                upload.title = node.childNodes[j].firstChild.data;
                            }
                        }
                        settings.documents.push(upload);                         
                    }
                }
                settings.documents.forEach(upload => {
                    $(settings.uploadsTbl).append("<tr><td>{0}</td><td>{1}</td><td><input type=\"text\" id=\"hybris-contextdocuments-document-{0}\" class=\"path\" text=\"{2}\"/></td><td><button id=\"hybris-contextdocuments-selectFile-{0}\">Browse</button></td></tr>".format(upload.id,upload.title,upload.path));
                    document.getElementById("hybris-contextdocuments-selectFile-{0}".format(upload.id)).addEventListener('click', (event) => {
                        ipcRenderer.send('contextdocuments-open-file-dialog',upload.id);
                    })
                }); 
            }
            else {
                ipcRenderer.send('open-error-dialog',settings.outputEmptyFileTitle.innerHTML,settings.outputEmptyFileText.innerHTML);
            }
        }

        //Upload documents
        var EnsureDocuments = function () {
            settings.documents.forEach(upload => {
                if(upload.path !== "") {
                    var folders = upload.path.getFolders();
                    var fileName = folders[folders.length - 1];
                    var fileExt = fileName.substring(fileName.indexOf(".") + 1);
                    var path = (settings.documentsPath.indexOf("\\") > -1) ? "{0}\\{1}\\1.{2}".format(settings.documentsPath,upload.id,fileExt) : "{0}/{1}/1.{2}".format(settings.documentsPath,upload.id,fileExt);
                    console.log(`copy file: ${fileName} to ${path}`);
                    fs.copyFileSync(upload.path, path);
                }
            });
        }

        //Ensure documents folder Structure 
        var EnsureStructure = function () {
            var destPath = settings.structureCallback().deliveryPackagePath;
            settings.documentsPath = (destPath.indexOf("\\") > -1) ? "{0}\\{1}\\{2}".format(destPath,settings.contextDocumentationFolder,settings.docCollectionFolderName) : "{0}/{1}/{2}".format(destPath,settings.contextDocumentationFolder,settings.docCollectionFolderName);
            if(!fs.existsSync(settings.documentsPath)) {
                console.log(`Create documents Path: ${settings.filePath}`);
                fs.mkdirSync(settings.documentsPath, { recursive: true });
            }
            var path = null;
            settings.documents.forEach(upload => {
                path = (settings.documentsPath.indexOf("\\") > -1) ? "{0}\\{1}".format(settings.documentsPath,upload.id) : "{0}/{1}".format(settings.documentsPath,upload.id);
                if(!fs.existsSync(path)) {                        
                    console.log(`create document path: ${path}`);
                    fs.mkdirSync(path, { recursive: true });
                }
            });
        }

        //commit print data
        var EnsureData = function() {
            var data = fs.readFileSync(settings.filePath);        
            var folders = settings.structureCallback().deliveryPackagePath.getFolders();
            var folderName = folders[folders.length - 1];
            var updatedData = data.toString().format(folderName,settings.logs.join("\r\n"));
            fs.writeFileSync(settings.filePath, updatedData);                         
            settings.logs = [];                               
        }

        //copy HTML template file to parent folder of selected Delivery Package folder
        var CopyFile = function() {
            var filePath = settings.scriptPath.format(settings.templateFileName);        
            if(!fs.existsSync(filePath)) {
                var rootPath = null;
                if(os.platform() == "win32") {
                    rootPath = path.join('./');
                    filePath = path.join(rootPath,settings.resourceWinPath.format(settings.templateFileName));
                }
                if(os.platform() == "darwin") {
                    var folders =  __dirname.split("/");
                    rootPath = folders.slice(0,folders.length - 3).join("/");
                    filePath = "{0}/{1}".format(rootPath,settings.templateFileName);
                }
            }        
            console.log(`copy ${settings.templateFileName} file to: ${settings.filePath}`);
            fs.copyFileSync(filePath, settings.filePath);
            EnsureData();        
        }

        //enable/diable waiting spinner
        var UpdateSpinner = function(spinnerClass) {
            var disabled = (spinnerClass === "") ? false : true;
            settings.spinner.className = spinnerClass;
            settings.okBtn.disabled = disabled;
            settings.printBtn.disabled = disabled;
            settings.nextBtn.disabled = disabled;
            $("button[id^='hybris-contextdocuments-selectFile-']").each(function() {
                this.disabled = disabled;
            });                    
        }

        //add Event Listener to HTML elmenets
        var AddEvents = function () {
            settings.nextBtn.addEventListener('click', function (event) {
                settings.overviewTab.click();
            });
            settings.okBtn.addEventListener('click', function (event) {
                if(settings.documents.length > 0) {                    
                    UpdateSpinner(settings.spinnerClass);
                    EnsureStructure();
                    EnsureDocuments();
                    UpdateSpinner("");                    
                    ipcRenderer.send('open-information-dialog',settings.outputOkInformationTitle.innerHTML,settings.outputOkInformationText.innerHTML);
                }
            });
            settings.printBtn.addEventListener('click', function (event) {
                settings.filePath = settings.filePostfix.format(settings.structureCallback().deliveryPackagePath);
                settings.documents.forEach(upload => {
                    settings.logs.push("<tr><td>{0}</td><td>{1}</td><td>{2}</td></tr>".format(upload.id,upload.title,upload.path));
                });
                CopyFile();
                shell.openItem(settings.filePath);
            });
            ipcRenderer.on('contextdocuments-selected-file', (event, path, id) => {
                console.log(`selected document ${id} with path: ${path}`);
                var upload = GetDocument(id);
                upload.path = path[0]; 
                document.getElementById("hybris-contextdocuments-document-{0}".format(upload.id)).value = upload.path;          
            })
        }
        
        //Model interfaces functions
        Rigsarkiv.Hybris.ContextDocuments = {
            initialize: function (structureCallback,outputErrorId,okId,uploadsId,printId,outputEmptyFileId,nextId,overviewTabId,outputOkInformationPrefixId,spinnerId) {
                settings.structureCallback = structureCallback;
                settings.okBtn = document.getElementById(okId);
                settings.outputErrorSpn =  document.getElementById(outputErrorId);
                settings.outputErrorText = settings.outputErrorSpn.innerHTML;
                settings.uploadsTbl = document.getElementById(uploadsId);
                settings.printBtn = document.getElementById(printId);
                settings.outputEmptyFileTitle = document.getElementById(outputEmptyFileId + "-Title");
                settings.outputEmptyFileText = document.getElementById(outputEmptyFileId + "-Text");
                settings.nextBtn = document.getElementById(nextId);
                settings.overviewTab = document.getElementById(overviewTabId);
                settings.outputOkInformationTitle = document.getElementById(outputOkInformationPrefixId + "-Title");
                settings.outputOkInformationText = document.getElementById(outputOkInformationPrefixId + "-Text");
                settings.spinner = document.getElementById(spinnerId);
                settings.spinnerClass = settings.spinner.className;
                settings.spinner.className = "";
                AddEvents();
            },
            callback: function () {
                return {
                    load: function(data) {
                        try {
                            Reset();
                            RenderDocuments(data);
                        }
                        catch(err) {
                            HandleError(err);
                        } 
                    }
                }
            }
        }
    }(jQuery);
}(jQuery);