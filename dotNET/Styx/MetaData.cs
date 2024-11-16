using Rigsarkiv.Asta.Logging;
using Rigsarkiv.Styx.Entities;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml.Linq;

namespace Rigsarkiv.Styx
{
    /// <summary>
    /// MetaData Converter
    /// </summary>
    public class MetaData : Converter
    {
        const string VariablesPath = "{0}\\Data\\{1}_{2}\\{1}_{2}_VARIABEL.txt";
        const string DescriptionsPath = "{0}\\Data\\{1}_{2}\\{1}_{2}_VARIABELBESKRIVELSE.txt";
        const string OldTypeStringPattern = "^(CHAR|CHARACTER|CHAR VARYING|CHARACTER VARYING|VARCHAR|NATIONAL CHARACTER|NATIONAL CHAR|NCHAR|NATIONAL CHARACTER VARYING|NATIONAL CHAR VARYING|NCHAR VARYING)[\\w\\W]*$";
        const string OldTypeIntPattern = "^(INTEGER|INT|SMALLINT)$";
        const string OldTypeDecimalPattern = "^(NUMERIC|DECIMAL|DEC|FLOAT|REAL|DOUBLE PRECISION)$";
        const string OldTypeBooleanPattern = "^(BOOLEAN)$";
        const string OldTypeDatePattern = "^(DATE)$";
        const string OldTypeTimePattern = "^(TIME|TIME\\[WITH TIME ZONE\\])$";
        const string OldTypeDateTimePattern = "^(TIMESTAMP|TIMESTAMP\\[WITH TIME ZONE\\])$";
        const int VariableDescriptionMaxLength = 256;
        private StringBuilder _variables = null;
        private StringBuilder _descriptions = null;
        private StringBuilder _codeList = null;
        private StringBuilder _usercodes = null;

        public MetaData(LogManager logManager, string srcPath, string destPath, string destFolder, Report report, FlowState state) : base(logManager, srcPath, destPath, destFolder)
        {
            _logSection = "Metadata";
            _report = report;
            _state = state;
            _variables = new StringBuilder();
            _descriptions = new StringBuilder();
            _codeList = new StringBuilder();
            _usercodes = new StringBuilder();
        }

        /// <summary>
        /// start converter
        /// </summary>
        public override bool Run()
        {
            var result = false;
            var message = string.Format("Start Converting Metadata {0} -> {1}", _srcFolder, _destFolder);
            _log.Info(message);
            _logManager.Add(new LogEntity() { Level = LogLevel.Info, Section = _logSection, Message = message });
            result = (_state == FlowState.Running || _state == FlowState.Suspended) ? EnsureTables() : true;
            if ((_state == FlowState.Running || _state == FlowState.Completed) && result)
            {
                result = EnsureFiles();
            }
            message = result ? "End Converting Metadata" : "End Converting Metadata with errors";
            _log.Info(message);
            _logManager.Add(new LogEntity() { Level = LogLevel.Info, Section = _logSection, Message = message });
            return result;
        }

        private bool EnsureFiles()
        {
            var result = true;
            try
            {
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Debug,
                    Section = _logSection,
                    Message = $"Starting file generation for {_report.Tables.Count} tables"
                });

                _report.Tables.ForEach(table =>
                {
                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Info,
                        Section = _logSection,
                        Message = $"Processing files for table: {table.Folder}"
                    });

                    _variables.Clear();
                    _descriptions.Clear();
                    _codeList.Clear();
                    _usercodes.Clear();

                    EnsureFiles(table);

                    if (_report.ScriptType == ScriptType.SPSS)
                    {
                        _logManager.Add(new LogEntity()
                        {
                            Level = LogLevel.Debug,
                            Section = _logSection,
                            Message = $"Generating SPSS user codes for table {table.Name}"
                        });
                        EnsureUserCodesFile(table);
                    }
                });

                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Debug,
                    Section = _logSection,
                    Message = "File generation completed successfully"
                });
            }
            catch (Exception ex)
            {
                result = false;
                _log.Error("EnsureFiles Failed", ex);
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Error,
                    Section = _logSection,
                    Message = $"EnsureFiles Failed: {ex.Message}\nStack Trace: {ex.StackTrace}"
                });
            }
            return result;
        }

        private void EnsureUserCodesFile(Table table)
        {
            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Debug,
                Section = _logSection,
                Message = $"Starting user codes generation for table {table.Name}"
            });

            var columnsWithMissingValues = table.Columns.Where(c => c.MissingValues != null).ToList();

            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Debug,
                Section = _logSection,
                Message = $"Found {columnsWithMissingValues.Count} columns with missing values in {table.Name}"
            });

            var index = 0;
            columnsWithMissingValues.ForEach(column =>
            {
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Debug,
                    Section = _logSection,
                    Message = $"Processing user codes for column: {column.Name} at index {index}"
                });

                _usercodes.AppendLine(string.Format("{0} {{{1}}}", NormalizeName(column.Name), index));
                index++;
            });

            if (_usercodes.Length > 0)
            {
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Info,
                    Section = _logSection,
                    Message = $"Writing user codes file for table {table.Name}"
                });
                EnsureFile(table, UserCodesPath, _usercodes.ToString());
            }
            else
            {
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Debug,
                    Section = _logSection,
                    Message = $"No user codes to write for table {table.Name}"
                });
            }
        }

        private void EnsureFiles(Table table)
        {
            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Debug,
                Section = _logSection,
                Message = $"Starting file generation for table {table.Name}"
            });

            var index = 0;
            table.Columns.ForEach(column =>
            {
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Debug,
                    Section = _logSection,
                    Message = $"Processing column: {column.Name} ({column.TypeOriginal})"
                });

                var codeList = string.Empty;
                if (column.CodeList != null)
                {
                    var codelistName = NormalizeName(column.CodeList.Name);
                    if (_report.ScriptType == ScriptType.SPSS)
                    {
                        codelistName = NormalizeName(column.Name);
                        _logManager.Add(new LogEntity()
                        {
                            Level = LogLevel.Debug,
                            Section = _logSection,
                            Message = $"Using SPSS codelist name for {column.Name}: {codelistName}"
                        });
                    }

                    codeList = string.Format("{0}{1}.",
                        column.TypeOriginal.StartsWith("VARCHAR") ? "$" : string.Empty,
                        codelistName);
                    _codeList.AppendLine(codelistName.Replace("\'", "\'\'"));
                    _codeList.AppendLine(string.Format("{{{0}}}", index));
                    index++;

                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Debug,
                        Section = _logSection,
                        Message = $"Added codelist entry for {column.Name}: {codeList}"
                    });
                }

                _variables.AppendLine(string.Format("{0} {1} {2}",
                    NormalizeName(column.Name),
                    GetColumnType(column),
                    codeList.Replace("\'", "\'\'")));

                _descriptions.AppendLine(string.Format("{0} '{1}'",
                    NormalizeName(column.Name),
                    EnsureNewLines(column.Description).Replace("\'", "\'\'")));

                CheckDescriptionLength(column);
            });

            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Info,
                Section = _logSection,
                Message = $"Writing files for table {table.Name}"
            });

            EnsureFile(table, VariablesPath, _variables.ToString());
            EnsureFile(table, DescriptionsPath, _descriptions.ToString());
            EnsureFile(table, CodeListPath, _codeList.ToString());
        }


        private void CheckDescriptionLength(Column column)
        {
            var descriptionLength = Encoding.UTF8.GetByteCount(column.Description);

            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Debug,
                Section = _logSection,
                Message = $"Checking description length for {column.Name}: {descriptionLength} bytes"
            });

            if (descriptionLength > VariableDescriptionMaxLength)
            {
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Warning,
                    Section = _logSection,
                    Message = $"Variable description for {column.Name} exceeds maximum length. Length: {descriptionLength}, Max: {VariableDescriptionMaxLength}"
                });

                column.VariableDescriptionLengthExceeded = descriptionLength;
            }
        }

        private void EnsureFile(Table table, string filePath, string content)
        {
            var path = string.Format(filePath, _destFolderPath,
                _report.ScriptType.ToString().ToLower(),
                _state == FlowState.Completed ? NormalizeName(table.Name) : NormalizeName(table.Name));

            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Debug,
                Section = _logSection,
                Message = $"Writing file at path: {path}, Content length: {content.Length}"
            });

            try
            {
                using (var sw = new StreamWriter(path, true, Encoding.UTF8))
                {
                    sw.Write(content);
                }

                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Info,
                    Section = _logSection,
                    Message = $"Successfully wrote file: {path}"
                });
            }
            catch (Exception ex)
            {
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Error,
                    Section = _logSection,
                    Message = $"Failed to write file {path}: {ex.Message}"
                });
                throw;
            }
        }

        private bool EnsureTables()
        {
            var result = true;
            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Debug,
                Section = _logSection,
                Message = $"Starting tables metadata processing in {_state} state"
            });

            try
            {
                if (_state == FlowState.Running && _researchIndexXDocument == null)
                {
                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Error,
                        Section = _logSection,
                        Message = "ResearchIndexXDocument property not set in Running state"
                    });
                    throw new Exception("ResearchIndexXDocument property not set");
                }

                var path = string.Format(DataPath, _destFolderPath);

                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Info,
                    Section = _logSection,
                    Message = $"Processing tables metadata at path: {path}"
                });

                _report.Tables.ForEach(table =>
                {
                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Info,
                        Section = _logSection,
                        Message = $"Building metadata for table: {table.Folder}"
                    });

                    var tableNode = _tableIndexXDocument.Element(_tableIndexXNS + "siardDiark")
                        .Element(_tableIndexXNS + "tables")
                        .Elements()
                        .Where(e => e.Element(_tableIndexXNS + "folder").Value == table.SrcFolder)
                        .FirstOrDefault();

                    if (tableNode == null)
                    {
                        _logManager.Add(new LogEntity()
                        {
                            Level = LogLevel.Warning,
                            Section = _logSection,
                            Message = $"Could not find table node for folder: {table.SrcFolder}"
                        });
                        return; // continue with next table
                    }

                    XElement researchNode = null;
                    if (_state == FlowState.Running)
                    {
                        researchNode = _researchIndexXDocument.Element(_tableIndexXNS + "researchIndex")
                            .Element(_tableIndexXNS + "mainTables")
                            .Elements()
                            .Where(e => e.Element(_tableIndexXNS + "tableID").Value == table.SrcFolder)
                            .FirstOrDefault();

                        _logManager.Add(new LogEntity()
                        {
                            Level = LogLevel.Debug,
                            Section = _logSection,
                            Message = $"Found research node for table: {table.Name}"
                        });
                    }

                    var columnCount = tableNode.Element(_tableIndexXNS + "columns").Elements().Count();
                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Debug,
                        Section = _logSection,
                        Message = $"Processing {columnCount} columns for table {table.Name}"
                    });

                    foreach (var columnNode in tableNode.Element(_tableIndexXNS + "columns").Elements())
                    {
                        var columnName = columnNode.Element(_tableIndexXNS + "name").Value;
                        _logManager.Add(new LogEntity()
                        {
                            Level = LogLevel.Debug,
                            Section = _logSection,
                            Message = $"Processing column: {columnName}"
                        });

                        var column = new Column()
                        {
                            Id = columnNode.Element(_tableIndexXNS + "columnID").Value,
                            Name = columnName,
                            Description = columnNode.Element(_tableIndexXNS + "description").Value,
                            Type = columnNode.Element(_tableIndexXNS + "typeOriginal").Value,
                            TypeOriginal = columnNode.Element(_tableIndexXNS + "type").Value
                        };

                        column.IsKey = tableNode.Element(_tableIndexXNS + "primaryKey")
                            .Element(_tableIndexXNS + "column").Value == column.Name;

                        if (column.IsKey)
                        {
                            _logManager.Add(new LogEntity()
                            {
                                Level = LogLevel.Debug,
                                Section = _logSection,
                                Message = $"Column {columnName} is primary key"
                            });
                        }

                        EnsureType(column);

                        var hasForeignKey = tableNode.Element(_tableIndexXNS + "foreignKeys")
                            .Elements()
                            .Any(e => e.Element(_tableIndexXNS + "reference")
                                .Element(_tableIndexXNS + "column").Value == column.Name);

                        if (hasForeignKey)
                        {
                            _logManager.Add(new LogEntity()
                            {
                                Level = LogLevel.Debug,
                                Section = _logSection,
                                Message = $"Processing foreign key for column {columnName}"
                            });

                            var foreignKeyNode = tableNode.Element(_tableIndexXNS + "foreignKeys")
                                .Elements()
                                .Where(e => e.Element(_tableIndexXNS + "reference")
                                    .Element(_tableIndexXNS + "column").Value == column.Name)
                                .FirstOrDefault();

                            column.CodeList = GetCodeList(foreignKeyNode, table, column);
                        }

                        if (_state == FlowState.Running)
                        {
                            _logManager.Add(new LogEntity()
                            {
                                Level = LogLevel.Debug,
                                Section = _logSection,
                                Message = $"Getting missing values for column {columnName}"
                            });
                            column.MissingValues = GetMissingValues(researchNode, table, column);
                        }

                        table.Columns.Add(column);
                        _logManager.Add(new LogEntity()
                        {
                            Level = LogLevel.Debug,
                            Section = _logSection,
                            Message = $"Successfully added column {columnName} to table {table.Name}"
                        });
                    }

                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Info,
                        Section = _logSection,
                        Message = $"Completed metadata processing for table {table.Name} with {table.Columns.Count} columns"
                    });
                });

                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Debug,
                    Section = _logSection,
                    Message = "Tables metadata processing completed successfully"
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
                    Message = $"EnsureTables Failed: {ex.Message}\nStack Trace: {ex.StackTrace}"
                });
            }
            return result;
        }

        private void EnsureType(Column column)
        {
            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Debug,
                Section = _logSection,
                Message = $"Starting type verification for column {column.Name}, Current type: {column.TypeOriginal}"
            });

            if (_state == FlowState.Suspended)
            {
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Debug,
                    Section = _logSection,
                    Message = $"Updating old type format for column {column.Name}"
                });
                UpdateOldType(column);
            }

            if (column.TypeOriginal.StartsWith(VarCharPrefix))
            {
                var regex = GetRegex(DataTypeIntPattern);
                if (regex.IsMatch(column.Type))
                {
                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Info,
                        Section = _logSection,
                        Message = $"Converting column {column.Name} type from VARCHAR to INTEGER"
                    });
                    column.TypeOriginal = "INTEGER";
                    column.Modified = true;
                    return;
                }

                regex = GetRegex(DataTypeDecimalPattern);
                if (regex.IsMatch(column.Type))
                {
                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Info,
                        Section = _logSection,
                        Message = $"Converting column {column.Name} type from VARCHAR to DECIMAL"
                    });
                    column.TypeOriginal = "DECIMAL";
                    column.Modified = true;
                    return;
                }
            }
        }

        private void UpdateOldType(Column column)
        {
            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Debug,
                Section = _logSection,
                Message = $"Starting old type update for column {column.Name}, Original type: {column.TypeOriginal}"
            });

            var result = string.Empty;
            var regex = GetRegex(OldTypeStringPattern);

            if (regex.IsMatch(column.TypeOriginal))
            {
                result = string.Format("VARCHAR({0})", StringMaxLength);
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Debug,
                    Section = _logSection,
                    Message = $"Matched string pattern for {column.Name}, new type: {result}"
                });
            }

            // Lignende logging for hver type-match
            regex = GetRegex(OldTypeIntPattern);
            if (regex.IsMatch(column.TypeOriginal))
            {
                result = "INTEGER";
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Debug,
                    Section = _logSection,
                    Message = $"Matched integer pattern for {column.Name}"
                });
            }

            if (!string.IsNullOrEmpty(result))
            {
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Info,
                    Section = _logSection,
                    Message = $"Updated column {column.Name} type from {column.TypeOriginal} to {result}"
                });
                column.Type = string.Empty;
                column.TypeOriginal = result;
                column.Modified = true;
            }
            else
            {
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Debug,
                    Section = _logSection,
                    Message = $"No type update needed for column {column.Name}"
                });
            }
        }

        private Dictionary<string, string> GetMissingValues(XElement tableNode, Table table, Column column)
        {
            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Debug,
                Section = _logSection,
                Message = $"Retrieving missing values for column {column.Name} in table {table.Name}"
            });

            if (tableNode.Element(_tableIndexXNS + "columns") == null)
            {
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Debug,
                    Section = _logSection,
                    Message = $"No columns element found for table {table.Name}"
                });
                return null;
            }

            if (!tableNode.Element(_tableIndexXNS + "columns").Elements().Any(e => e.Element(_tableIndexXNS + "columnID").Value == column.Id))
            {
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Debug,
                    Section = _logSection,
                    Message = $"Column {column.Name} not found in table node"
                });
                return null;
            }

            var result = new Dictionary<string, string>();
            var columnNode = tableNode.Element(_tableIndexXNS + "columns")
                .Elements()
                .Where(e => e.Element(_tableIndexXNS + "columnID").Value == column.Id)
                .FirstOrDefault();

            var missingValuesCount = columnNode.Element(_tableIndexXNS + "missingValues")?.Elements().Count() ?? 0;
            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Info,
                Section = _logSection,
                Message = $"Found {missingValuesCount} missing values for column {column.Name}"
            });

            foreach (var valueNode in columnNode.Element(_tableIndexXNS + "missingValues").Elements())
            {
                result.Add(valueNode.Value, valueNode.Value);
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Debug,
                    Section = _logSection,
                    Message = $"Added missing value: {valueNode.Value} for column {column.Name}"
                });
            }

            return result;
        }

        private Table GetCodeList(XElement foreignKeyNode, Table table, Column column)
        {
            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Debug,
                Section = _logSection,
                Message = $"Starting code list retrieval for column {column.Name} in table {table.Name}"
            });

            var referencedTable = foreignKeyNode.Element(_tableIndexXNS + "referencedTable").Value;

            if (_report.Tables.Any(t => t.Name == referencedTable))
            {
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Debug,
                    Section = _logSection,
                    Message = $"Referenced table {referencedTable} already exists in report tables - skipping"
                });
                return null;
            }

            var result = new Table()
            {
                Name = NormalizeName(referencedTable),
                Columns = new List<Column>(),
                RowsCounter = 0
            };

            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Info,
                Section = _logSection,
                Message = $"Creating new code list table: {result.Name} for column {column.Name}"
            });

            var tableNode = _tableIndexXDocument.Element(_tableIndexXNS + "siardDiark")
                .Element(_tableIndexXNS + "tables")
                .Elements()
                .Where(e => e.Element(_tableIndexXNS + "name").Value == referencedTable)
                .FirstOrDefault();

            if (tableNode == null)
            {
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Warning,
                    Section = _logSection,
                    Message = $"Could not find referenced table node for {referencedTable}"
                });
                return null;
            }

            result.SrcFolder = tableNode.Element(_tableIndexXNS + "folder").Value;
            result.Rows = int.Parse(tableNode.Element(_tableIndexXNS + "rows").Value);

            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Debug,
                Section = _logSection,
                Message = $"Processing {result.Rows} rows from source folder: {result.SrcFolder}"
            });

            foreach (var columnNode in tableNode.Element(_tableIndexXNS + "columns").Elements())
            {
                var columnId = columnNode.Element(_tableIndexXNS + "columnID").Value;
                var columnName = columnNode.Element(_tableIndexXNS + "name").Value;

                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Debug,
                    Section = _logSection,
                    Message = $"Processing code list column: {columnName} (ID: {columnId})"
                });

                var codeListColumn = new Column()
                {
                    Id = columnId,
                    Name = columnName,
                    Description = columnNode.Element(_tableIndexXNS + "description").Value,
                    Type = columnNode.Element(_tableIndexXNS + "typeOriginal").Value,
                    TypeOriginal = columnNode.Element(_tableIndexXNS + "type").Value
                };

                codeListColumn.IsKey = tableNode.Element(_tableIndexXNS + "primaryKey")
                    .Element(_tableIndexXNS + "column").Value == codeListColumn.Name;

                if (codeListColumn.IsKey)
                {
                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Debug,
                        Section = _logSection,
                        Message = $"Column {columnName} identified as primary key"
                    });
                }

                if (_state == FlowState.Suspended)
                {
                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Debug,
                        Section = _logSection,
                        Message = $"Updating old type format for column {columnName}"
                    });
                    UpdateOldType(codeListColumn);
                }

                CheckDescriptionLength(codeListColumn);
                result.Columns.Add(codeListColumn);

                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Debug,
                    Section = _logSection,
                    Message = $"Successfully added column {columnName} to code list table"
                });
            }

            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Info,
                Section = _logSection,
                Message = $"Completed code list table creation for {result.Name} with {result.Columns.Count} columns"
            });

            return result;
        }
    }
}
