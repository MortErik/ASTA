window.Rigsarkiv = window.Rigsarkiv || {},
function (n) {
    const {ipcRenderer} = require('electron')
    const {shell} = require('electron')
    const fs = require('fs');
    const pattern = /^([1-9]{1}[0-9]{4,})$/;

    var settings = {
        selectDirBtn: null,
        pathDirTxt: null,
        selectedPath: null,
        deliveryPackageTxt: null,
        okBtn: null,
        outputErrorSpn: null,
        outputErrorText: null,
        outputExistsSpn: null,
        outputExistsText: null,
        outputRequiredPathSpn: null,
        outputUnvalidDeliveryPackageSpn: null,
        outputOkSpn: null,
        outputOkText: null,
        selectDeliveryPackage: null,
        folderPrefix: "FD.",
        defaultFolderPostfix: "99999",
        subFolders: ["ContextDocumentation","Data","Indices"]
    }

    var Reset = function () {
        settings.outputErrorSpn.hidden = true;
        settings.outputExistsSpn.hidden = true;
        settings.outputRequiredPathSpn.hidden = true;
        settings.outputUnvalidDeliveryPackageSpn.hidden = true;
        settings.outputOkSpn.hidden = true;
        settings.selectDeliveryPackage.hidden = true;
    }

    var EnsureStructure = function () {
        var folderName = settings.folderPrefix;
        folderName += (settings.deliveryPackageTxt.value === "") ? settings.defaultFolderPostfix: settings.deliveryPackageTxt.value;
        var folderPath = settings.selectedPath + "\\" + folderName        
        fs.exists(folderPath, (exists) => {
            if(!exists) {
                console.log("Create structure: " + folderPath);
                fs.mkdir(folderPath, { recursive: true }, (err) => {
                    if (err) {
                        settings.outputErrorSpn.hidden = false;
                        settings.outputErrorSpn.innerHTML = settings.outputErrorText.format(err.message);
                    }
                });
                settings.subFolders.forEach(element => {
                    fs.mkdir(folderPath + "\\" + element, { recursive: true }, (err) => {
                        if (err) {
                            settings.outputErrorSpn.hidden = false;
                            settings.outputErrorSpn.innerHTML = settings.outputErrorText.format(err.message);
                        }
                    });
                });
                settings.selectDeliveryPackage.innerHTML = folderPath;
                settings.selectDeliveryPackage.hidden = false;
                settings.outputOkSpn.hidden = false;
                settings.outputOkSpn.innerHTML = settings.outputOkText.format(folderName);
            }
            else  {
                settings.outputExistsSpn.hidden = false;
                settings.outputExistsSpn.innerHTML = settings.outputExistsText.format(folderName);
            }
        });
    }

    var AddEvents = function () {
        settings.okBtn.addEventListener('click', (event) => {
            Reset();
            if(settings.pathDirTxt.value === "") {
                settings.outputRequiredPathSpn.hidden = false;
            }
            if(settings.deliveryPackageTxt.value !== "" && !pattern.test(settings.deliveryPackageTxt.value)) {
                settings.outputUnvalidDeliveryPackageSpn.hidden = false;
            }
            if(settings.selectedPath != null && settings.pathDirTxt.value !== "" && (settings.deliveryPackageTxt.value === "" || (settings.deliveryPackageTxt.value !== "" && pattern.test(settings.deliveryPackageTxt.value)))) {
               EnsureStructure();
            }
        })
        settings.selectDirBtn.addEventListener('click', (event) => {
            Reset();
            ipcRenderer.send('open-file-dialog');
        })
        ipcRenderer.on('selected-directory', (event, path) => {
            settings.selectedPath = path; 
            console.log(`selected path: ${path}`); 
            settings.pathDirTxt.value = settings.selectedPath;
         })
        settings.selectDeliveryPackage.addEventListener('click', (event) => {
            var folderName = settings.folderPrefix;
            folderName += (settings.deliveryPackageTxt.value === "") ? settings.defaultFolderPostfix: settings.deliveryPackageTxt.value;
            var folderPath = settings.selectedPath + "\\" + folderName
            shell.openItem(folderPath);
        }) 
    }

    Rigsarkiv.Structure = {        
        initialize: function (selectDirectoryId,pathDirectoryId,deliveryPackageId,okId,outputErrorId,outputExistsId,outputRequiredPathId,outputUnvalidDeliveryPackageId,outputOkId,selectDeliveryPackageId) {            
            settings.selectDirBtn =  document.getElementById(selectDirectoryId);
            settings.pathDirTxt =  document.getElementById(pathDirectoryId);
            settings.deliveryPackageTxt =  document.getElementById(deliveryPackageId);
            settings.okBtn =  document.getElementById(okId);
            settings.outputErrorSpn =  document.getElementById(outputErrorId);
            settings.outputErrorText = settings.outputErrorSpn.innerHTML;
            settings.outputExistsSpn =  document.getElementById(outputExistsId);
            settings.outputExistsText = settings.outputExistsSpn.innerHTML;
            settings.outputRequiredPathSpn =  document.getElementById(outputRequiredPathId);
            settings.outputUnvalidDeliveryPackageSpn =  document.getElementById(outputUnvalidDeliveryPackageId);
            settings.outputOkSpn =  document.getElementById(outputOkId);
            settings.outputOkText = settings.outputOkSpn.innerHTML;
            settings.selectDeliveryPackage = document.getElementById(selectDeliveryPackageId);
            AddEvents();
        }
    };
}(jQuery);