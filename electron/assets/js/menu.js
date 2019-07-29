/*
 Model is responsible for left menu
 menu is based on sections HTML pages 
 each page start with <template> tag with class "section-template" and others subs classes where menu events is based on
*/
window.navigation = window.navigation || {},
    function (n) {
        const {getCurrentWindow, globalShortcut} = require('electron').remote;
        const fs = require('fs');
        const path = require('path');
        const os = require('os');
        const { spawn } = require('child_process');
        navigation.menu = {
            constants: {
                sectionTemplate: '.section-template',
                contentContainer: '#content',
                startSectionMenuItem: "#welcome-menu",
                startSection: "#welcome",
                scriptPath: "./assets/scripts/{0}",
                resourceWinPath: "resources\\{0}",
                converterFileName: "StyxForm.exe"
            },

            importSectionsToDOM: function () {
                const links = document.querySelectorAll('link[rel="import"]')
                Array.prototype.forEach.call(links, function (link) {
                    let template = link.import.querySelector(navigation.menu.constants.sectionTemplate)
                    let clone = document.importNode(template.content, true)
                    document.querySelector(navigation.menu.constants.contentContainer).appendChild(clone)
                })
            },

            setMenuOnClickEvent: function () {
                document.body.addEventListener('click', function (event) {
                    if (event.target.dataset.section) {
                        $('#side-menu').find('.selected').removeClass('selected');
                        navigation.menu.hideAllSections()
                        navigation.menu.showSection(event)
                    }
                })
            },

            showSection: function (event) {
                const sectionId = event.target.dataset.section
                $('#' + sectionId).show()
                $('#' + sectionId + ' section').show()
                $(event.target).addClass('selected');
            },

            showStartSection: function () {
                $(this.constants.startSectionMenuItem).click()
                $(this.constants.startSection).show()
                $(this.constants.startSection + ' section').show()
                $(this.constants.startSectionMenuItem).addClass('selected');
            },

            hideAllSections: function () {
                $(this.constants.contentContainer + ' section').hide()
            },

            init: function () {
                this.importSectionsToDOM()
                this.setMenuOnClickEvent()
                this.hideAllSections()
                this.showStartSection()
            }
        };

        n(function () {
            var outputErrorSpn = document.getElementById("menu-output-Error");
            var outputErrorText = outputErrorSpn.innerHTML;
            Rigsarkiv.Rights.initialize("menu-output-Error");
            navigation.menu.init();
            document.getElementById("menu-reload").addEventListener('click', function (event) {
                getCurrentWindow().reload();
            });
            styxLink = document.getElementById("styx-menu");
            styxLink.addEventListener('click', function (event) {
                var converterFilePath = navigation.menu.constants.scriptPath.format(navigation.menu.constants.converterFileName);        
                if(!fs.existsSync(converterFilePath)) {
                    var rootPath = null;
                    if(os.platform() == "win32") {
                        rootPath = path.join('./');
                        converterFilePath = path.join(rootPath,navigation.menu.constants.resourceWinPath.format(navigation.menu.constants.converterFileName));
                    }
                    if(os.platform() == "darwin") {
                        var folders =  __dirname.split("/");
                        rootPath = folders.slice(0,folders.length - 3).join("/");
                        converterFilePath = "{0}/{1}".format(rootPath,navigation.menu.constants.converterFileName);
                    }
                }   
                var converter = spawn(converterFilePath);
                converter.stdout.on('data', (data) => console.log(`stdout: ${data}`));                  
                converter.stderr.on('data', (data) => (new Error(data).Handle(outputErrorSpn,outputErrorText,"Rigsarkiv.Nemesis.Data.AddEvents")));
                converter.on('close', (code) => console.log(`converter process exited with code ${code}`));
            });
            $(styxLink).hide();
            if(Rigsarkiv.Rights.callback().isAdmin) {
                $(styxLink).show();
            }
        })

    }(jQuery);