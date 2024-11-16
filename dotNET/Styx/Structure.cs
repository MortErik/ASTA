using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml.Linq;
using Rigsarkiv.Asta.Logging;
using Rigsarkiv.Styx.Entities;

namespace Rigsarkiv.Styx
{
    /// <summary>
    /// Structure Converter
    /// </summary>
    public class Structure : Converter
    {
        const string TableIndexPath = "{0}\\Indices\\tableIndex.xml";
        const string ResearchIndexPath = "{0}\\Indices\\researchIndex.xml";
        const string TableFolderPrefix = "{0}_{1}";
        const string ContextDocumentationIndexPath = "{0}\\Indices\\contextDocumentationIndex.xml";
        const string ContextDocumentationPath = "{0}\\ContextDocumentation";
        const string ContextDocumentationPattern = "^[1-9]{1}[0-9]{0,}.(tif|mpg|mp3|jpg|jp2)$";
        private XDocument _contextDocumentationIndexXDocument = null;
        private Regex _contextDocumentationRegex = null;

        /// <summary>
        /// Constructore
        /// </summary>
        /// <param name="logManager"></param>
        /// <param name="srcPath"></param>
        /// <param name="destPath"></param>
        /// <param name="destFolder"></param>
        public Structure(LogManager logManager, string srcPath, string destPath, string destFolder, Report report, FlowState state) : base(logManager, srcPath, destPath, destFolder)
        {
            _logSection = "Structure";
            if (report != null) { _report = report; }
            _state = state;
            _contextDocumentationRegex = new Regex(ContextDocumentationPattern, RegexOptions.Compiled | RegexOptions.IgnoreCase);
        }

        /// <summary>
        /// start converter
        /// </summary>
        public override bool Run()
        {
            var result = false;
            var message = string.Format("Start Converting structure {0} -> {1}", _srcFolder, _destFolder);
            _log.Info(message);
            _logManager.Add(new LogEntity() { Level = LogLevel.Info, Section = _logSection, Message = message });
            if (EnsureRootFolder())
            {
                if (_state == FlowState.Created) { _state = File.Exists(string.Format(ResearchIndexPath, _srcPath)) ? FlowState.Running : FlowState.Suspended; }
                if (_state == FlowState.Suspended) { _logManager.Add(new LogEntity() { Level = LogLevel.Warning, Section = _logSection, Message = "No Research Index file found" }); }
                result = EnsureTables();
                if ((_state == FlowState.Running || _state == FlowState.Completed) && result)
                {
                    result = EnsureScripts() && CopyFiles();
                }
            }
            message = result ? "End Converting structure" : "End Converting structure with errors";
            _log.Info(message);
            _logManager.Add(new LogEntity() { Level = LogLevel.Info, Section = _logSection, Message = message });
            return result;
        }

        private bool EnsureRootFolder()
        {
            var result = true;
            try
            {
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Debug,
                    Section = _logSection,
                    Message = $"Starting root folder initialization at {_destFolderPath}"
                });

                if (Directory.Exists(_destFolderPath))
                {
                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Warning,
                        Section = _logSection,
                        Message = $"Delete existing path: {_destFolderPath}"
                    });

                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Debug,
                        Section = _logSection,
                        Message = $"Beginning directory deletion for {_destFolderPath}"
                    });
                    Directory.Delete(_destFolderPath, true);
                }

                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Info,
                    Section = _logSection,
                    Message = $"Creating path: {_destFolderPath}"
                });
                Directory.CreateDirectory(_destFolderPath);

                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Debug,
                    Section = _logSection,
                    Message = $"Root folder initialization completed successfully"
                });
            }
            catch (Exception ex)
            {
                result = false;
                _log.Error("EnsureRootFolder Failed", ex);
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Error,
                    Section = _logSection,
                    Message = $"EnsureRootFolder Failed: {ex.Message}"
                });
            }
            return result;
        }

        private bool CopyFiles()
        {
            var result = true;
            var srcPath = string.Format(ContextDocumentationPath, _srcPath);
            var destPath = string.Format(ContextDocumentationPath, _destFolderPath);

            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Debug,
                Section = _logSection,
                Message = $"Starting file copy operation\nSource: {srcPath}\nDestination: {destPath}"
            });

            try
            {
                // Opret destinationsmappe hvis den ikke findes
                if (Directory.Exists(destPath))
                {
                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Debug,
                        Section = _logSection,
                        Message = $"Destination folder already exists: {destPath}"
                    });
                }
                else
                {
                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Info,
                        Section = _logSection,
                        Message = $"Creating destination folder: {destPath}"
                    });
                    Directory.CreateDirectory(destPath);
                }

                // Ryd og initialiser
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Debug,
                    Section = _logSection,
                    Message = "Clearing existing context documents"
                });
                _report.ContextDocuments.Clear();

                // Hent filer
                var files = Getfiles();
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Debug,
                    Section = _logSection,
                    Message = $"Retrieved {files.Count} file groups"
                });

                // Load XML dokument
                _contextDocumentationIndexXDocument = XDocument.Load(string.Format(ContextDocumentationIndexPath, _srcPath));
                var documentNodes = _contextDocumentationIndexXDocument
                    .Element(_tableIndexXNS + "contextDocumentationIndex")
                    .Elements()
                    .ToList();

                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Debug,
                    Section = _logSection,
                    Message = $"Found {documentNodes.Count} document nodes in XML"
                });

                // Hold styr på filnummerering for hver dokumenttitel
                var titleCounters = new Dictionary<string, int>();

                // Process hver dokument node
                foreach (var documentNode in documentNodes)
                {
                    var id = documentNode.Element(_tableIndexXNS + "documentID").Value;
                    var documentTitle = documentNode.Element(_tableIndexXNS + "documentTitle").Value;
                    var title = ReplaceInvalidChars(documentTitle);

                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Debug,
                        Section = _logSection,
                        Message = $"Processing document:\nID: {id}\nOriginal Title: {documentTitle}\nNormalized Title: {title}"
                    });

                    if (files.ContainsKey(id) && files[id].Count > 0)
                    {
                        _logManager.Add(new LogEntity()
                        {
                            Level = LogLevel.Debug,
                            Section = _logSection,
                            Message = $"Found {files[id].Count} files for document ID: {id}"
                        });

                        var docFolderPath = string.Format("{0}\\{1}", destPath, title);
                        Directory.CreateDirectory(docFolderPath);

                        try
                        {
                            // Initialiser tæller hvis ikke eksisterer
                            if (!titleCounters.ContainsKey(documentTitle))
                            {
                                titleCounters[documentTitle] = 0;
                            }

                            // Increment tæller for denne titel
                            titleCounters[documentTitle]++;
                            var currentNumber = titleCounters[documentTitle];

                            var numberedTitle = $"{documentTitle}_{currentNumber}";

                            if (currentNumber > 1)
                            {
                                _logManager.Add(new LogEntity()
                                {
                                    Level = LogLevel.Warning,
                                    Section = _logSection,
                                    Message = $"Found duplicate document:\n" +
                                            $"ID: {id}\n" +
                                            $"Original Title: {documentTitle}\n" +
                                            $"Assigned Number: {currentNumber}"
                                });
                            }

                            // Tilføj til ContextDocuments med nummereret titel
                            _report.ContextDocuments.Add(numberedTitle, title);

                            _logManager.Add(new LogEntity()
                            {
                                Level = LogLevel.Info,
                                Section = _logSection,
                                Message = $"Added document to collection:\n" +
                                        $"Numbered Title: {numberedTitle}\n" +
                                        $"Original Title: {documentTitle}"
                            });

                            // Kopier alle filer med nummerering
                            foreach (var srcFilePath in files[id])
                            {
                                var fileExt = srcFilePath.Substring(srcFilePath.LastIndexOf(".") + 1);

                                // Simpel nummerering: 1.tif, 2.tif, etc.
                                var numberedFileName = $"{currentNumber}.{fileExt}";
                                var destFilePath = Path.Combine(docFolderPath, numberedFileName);

                                _logManager.Add(new LogEntity()
                                {
                                    Level = LogLevel.Info,
                                    Section = _logSection,
                                    Message = $"Copying file:\n" +
                                            $"From: {srcFilePath}\n" +
                                            $"To: {destFilePath}\n" +
                                            $"Number: {currentNumber}"
                                });

                                try
                                {
                                    File.Copy(srcFilePath, destFilePath, true);

                                    _logManager.Add(new LogEntity()
                                    {
                                        Level = LogLevel.Debug,
                                        Section = _logSection,
                                        Message = $"Successfully copied file as: {numberedFileName}"
                                    });
                                }
                                catch (Exception fileEx)
                                {
                                    _logManager.Add(new LogEntity()
                                    {
                                        Level = LogLevel.Error,
                                        Section = _logSection,
                                        Message = $"Failed to copy file:\n" +
                                                $"Source: {srcFilePath}\n" +
                                                $"Destination: {destFilePath}\n" +
                                                $"Error: {fileEx.Message}"
                                    });
                                    throw;
                                }
                            }
                        }
                        catch (Exception docEx)
                        {
                            _logManager.Add(new LogEntity()
                            {
                                Level = LogLevel.Error,
                                Section = _logSection,
                                Message = $"Error processing document:\n" +
                                        $"ID: {id}\n" +
                                        $"Title: {documentTitle}\n" +
                                        $"Error: {docEx.Message}\n" +
                                        $"Stack Trace: {docEx.StackTrace}"
                            });
                            throw;
                        }
                    }
                    else
                    {
                        _logManager.Add(new LogEntity()
                        {
                            Level = LogLevel.Debug,
                            Section = _logSection,
                            Message = $"No files found for document ID: {id}"
                        });
                    }
                }

                // Log statistik om dubletter
                var duplicateCount = titleCounters.Count(kvp => kvp.Value > 1);
                var totalDuplicateFiles = titleCounters.Sum(kvp => kvp.Value > 1 ? kvp.Value - 1 : 0);

                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Info,
                    Section = _logSection,
                    Message = $"File copy operation completed successfully.\n" +
                             $"Total documents processed: {_report.ContextDocuments.Count}\n" +
                             $"Unique document titles: {titleCounters.Count}\n" +
                             $"Titles with duplicates: {duplicateCount}\n" +
                             $"Total duplicate files: {totalDuplicateFiles}"
                });
            }
            catch (Exception ex)
            {
                result = false;
                _log.Error("CopyFiles Failed", ex);
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Error,
                    Section = _logSection,
                    Message = $"CopyFiles Failed:\n" +
                             $"Exception Type: {ex.GetType().Name}\n" +
                             $"Message: {ex.Message}\n" +
                             $"Stack Trace: {ex.StackTrace}"
                });
            }
            return result;
        }

        private string ReplaceInvalidChars(string fileName)
        {
            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Debug,
                Section = _logSection,
                Message = $"Starting character replacement for filename: {fileName}"
            });

            var result = string.Join("_", fileName.Split(Path.GetInvalidFileNameChars()));

            if (result != fileName)
            {
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Warning,
                    Section = _logSection,
                    Message = $"Filename required modification: {fileName} -> {result}"
                });

                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Debug,
                    Section = _logSection,
                    Message = $"Invalid characters were found and replaced in filename: {fileName}"
                });
            }
            else
            {
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Debug,
                    Section = _logSection,
                    Message = $"No invalid characters found in filename: {fileName}"
                });
            }

            return result;
        }

        private Dictionary<string, List<string>> Getfiles()
        {
            var result = new Dictionary<string, List<string>>();
            var srcPath = string.Format(ContextDocumentationPath, _srcPath);

            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Info,
                Section = _logSection,
                Message = $"Scanning files at: {srcPath}"
            });

            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Debug,
                Section = _logSection,
                Message = "Beginning file enumeration"
            });

            foreach (string filePath in Directory.GetFiles(srcPath, "*.*", SearchOption.AllDirectories))
            {
                var fileName = filePath.Substring(filePath.LastIndexOf("\\") + 1);

                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Debug,
                    Section = _logSection,
                    Message = $"Examining file: {fileName}"
                });

                if (_contextDocumentationRegex.IsMatch(fileName))
                {
                    var id = filePath.Substring(0, filePath.Length - (fileName.Length + 1));
                    id = id.Substring(id.LastIndexOf("\\") + 1);

                    if (!result.ContainsKey(id))
                    {
                        _logManager.Add(new LogEntity()
                        {
                            Level = LogLevel.Debug,
                            Section = _logSection,
                            Message = $"Adding new document group with ID: {id}"
                        });
                        result.Add(id, new List<string>());
                    }
                    result[id].Add(filePath);
                }
            }

            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Debug,
                Section = _logSection,
                Message = $"File scanning complete. Found {result.Count} document groups"
            });

            return result;
        }

        private List<XElement> GetTablesNodes()
        {
            var result = new List<XElement>();

            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Debug,
                Section = _logSection,
                Message = $"Loading table index document from {string.Format(TableIndexPath, _srcPath)}"
            });

            _tableIndexXDocument = XDocument.Load(string.Format(TableIndexPath, _srcPath));

            if (_state == FlowState.Running)
            {
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Debug,
                    Section = _logSection,
                    Message = "Processing in Running state - loading research index"
                });

                _researchIndexXDocument = XDocument.Load(string.Format(ResearchIndexPath, _srcPath));
                _researchIndexXDocument.Element(_tableIndexXNS + "researchIndex").Element(_tableIndexXNS + "mainTables").Elements().ToList().ForEach(tableNode => {
                    var srcFolder = tableNode.Element(_tableIndexXNS + "tableID").Value;
                    var tableIndexNode = _tableIndexXDocument.Element(_tableIndexXNS + "siardDiark").Element(_tableIndexXNS + "tables").Elements()
                        .Where(e => e.Element(_tableIndexXNS + "folder").Value == srcFolder).FirstOrDefault();

                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Debug,
                        Section = _logSection,
                        Message = $"Found table node for folder: {srcFolder}"
                    });

                    result.Add(tableIndexNode);
                });
            }
            else
            {
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Debug,
                    Section = _logSection,
                    Message = "Processing in non-Running state - checking foreign keys"
                });

                _tableIndexXDocument.Element(_tableIndexXNS + "siardDiark").Element(_tableIndexXNS + "tables").Elements().ToList().ForEach(tableNode => {
                    if (tableNode.Element(_tableIndexXNS + "foreignKeys") != null)
                    {
                        _logManager.Add(new LogEntity()
                        {
                            Level = LogLevel.Debug,
                            Section = _logSection,
                            Message = $"Found table with foreign keys: {tableNode.Element(_tableIndexXNS + "folder").Value}"
                        });
                        result.Add(tableNode);
                    }
                });
            }

            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Debug,
                Section = _logSection,
                Message = $"GetTablesNodes completed. Found {result.Count} tables"
            });

            return result;
        }

        private bool EnsureTables()
        {
            var result = true;
            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Debug,
                Section = _logSection,
                Message = "Starting tables initialization"
            });

            try
            {
                var path = string.Format(DataPath, _destFolderPath);
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Info,
                    Section = _logSection,
                    Message = $"Creating tables directory at: {path}"
                });
                Directory.CreateDirectory(path);

                if (_state == FlowState.Running || _state == FlowState.Suspended)
                {
                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Debug,
                        Section = _logSection,
                        Message = $"Processing tables in {_state} state"
                    });

                    var tableNodes = GetTablesNodes();
                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Debug,
                        Section = _logSection,
                        Message = $"Found {tableNodes.Count} table nodes to process"
                    });

                    foreach (var tableIndexNode in tableNodes)
                    {
                        var srcFolder = tableIndexNode.Element(_tableIndexXNS + "folder").Value;
                        var tableName = tableIndexNode.Element(_tableIndexXNS + "name").Value;
                        var tableRows = int.Parse(tableIndexNode.Element(_tableIndexXNS + "rows").Value);
                        var folder = string.Format(TableFolderPrefix, _report.ScriptType.ToString().ToLower(), NormalizeName(tableName));

                        _logManager.Add(new LogEntity()
                        {
                            Level = LogLevel.Debug,
                            Section = _logSection,
                            Message = $"Adding table: {tableName} from {srcFolder} with {tableRows} rows"
                        });

                        _report.Tables.Add(new Table()
                        {
                            Folder = folder,
                            SrcFolder = srcFolder,
                            Name = tableName,
                            Rows = tableRows,
                            RowsCounter = 0,
                            Columns = new List<Column>()
                        });
                    }
                }

                if (_state == FlowState.Running || _state == FlowState.Completed)
                {
                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Debug,
                        Section = _logSection,
                        Message = $"Creating table folders for {_report.Tables.Count} tables"
                    });

                    _report.Tables.ForEach(table => {
                        if (string.IsNullOrEmpty(table.Folder))
                        {
                            table.Folder = string.Format(TableFolderPrefix, _report.ScriptType.ToString().ToLower(), NormalizeName(table.Name));
                            _logManager.Add(new LogEntity()
                            {
                                Level = LogLevel.Debug,
                                Section = _logSection,
                                Message = $"Generated folder name for table {table.Name}: {table.Folder}"
                            });
                        }

                        var folderPath = string.Format("{0}\\{1}", path, table.Folder);
                        _logManager.Add(new LogEntity()
                        {
                            Level = LogLevel.Info,
                            Section = _logSection,
                            Message = $"Creating table folder: {folderPath}"
                        });
                        Directory.CreateDirectory(folderPath);
                    });
                }

                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Debug,
                    Section = _logSection,
                    Message = "Tables initialization completed successfully"
                });
            }
            catch (Exception ex)
            {
                result = false;
                _log.Error("EnsureTables Failed", ex);
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Error,
                    Section = _logSection,
                    Message = $"EnsureTables Failed: {ex.Message}"
                });
            }
            return result;
        }

        private bool EnsureScripts()
        {
            var result = true;
            string ext = null;

            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Debug,
                Section = _logSection,
                Message = $"Starting script generation for {_report.ScriptType}"
            });

            switch (_report.ScriptType)
            {
                case ScriptType.SPSS: ext = "sps"; break;
                case ScriptType.SAS: ext = "sas"; break;
                case ScriptType.Stata: ext = "do"; break;
            }

            try
            {
                if (_report.ScriptType == ScriptType.SPSS)
                {
                    var path = string.Format(DataPath, _destFolderPath);
                    var content = GetScriptTemplate();

                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Debug,
                        Section = _logSection,
                        Message = $"Generating SPSS scripts at {path}"
                    });

                    _report.Tables.ForEach(table =>
                    {
                        var folderPath = string.Format("{0}\\{1}", path, table.Folder);
                        var filePath = string.Format("{0}\\{1}.{2}", folderPath, table.Folder, ext);

                        _logManager.Add(new LogEntity()
                        {
                            Level = LogLevel.Info,
                            Section = _logSection,
                            Message = $"Creating script file: {filePath}"
                        });

                        _logManager.Add(new LogEntity()
                        {
                            Level = LogLevel.Debug,
                            Section = _logSection,
                            Message = $"Writing script content for table {table.Name}"
                        });

                        using (var sw = new StreamWriter(filePath, false, Encoding.UTF8))
                        {
                            sw.Write(string.Format(content, "", folderPath, table.Folder));
                        }
                    });

                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Debug,
                        Section = _logSection,
                        Message = "Script generation completed successfully"
                    });
                }
            }
            catch (Exception ex)
            {
                result = false;
                _log.Error("EnsureScripts Failed", ex);
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Error,
                    Section = _logSection,
                    Message = $"EnsureScripts Failed: {ex.Message}"
                });
            }
            return result;
        }
    }
}
