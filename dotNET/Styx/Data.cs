using Rigsarkiv.Asta.Logging;
using Rigsarkiv.Styx.Entities;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Web.Script.Serialization;
using System.Xml.Linq;

namespace Rigsarkiv.Styx
{
    /// <summary>
    /// Data converter
    /// </summary>
    public class Data : Converter
    {
        const int RowsChunk = 500;
        const string Separator = ";";
        const string ResourceReportFile = "Rigsarkiv.Styx.Resources.report.html";
        const string TablePath = "{0}\\Tables\\{1}\\{1}.xml";
        const string CodeFormat = "'{0}' '{1}'";
        const string SpecialNumericPattern = "^(\\.[a-z])|([A-Z])$";
        const string SasSpecialNumericPattern = "^[A-Z]$";
        const string StataSpecialNumericPattern = "^\\.[a-z]$";
        const string Alphabet = "abcdefghijklmnopqrstuvwxyz";
        const string UserCodeRange = "'{0}' 'through' '{1}'{2}";
        const string UserCodeExtra = ", '{0}'";
        const int CodeDescriptionMaxLength = 120;
        private List<string> _codeLists = null;
        private List<string> _sasSpecialNumerics = null;
        private List<string> _stataSpecialNumerics = null;

        /// <summary>
        /// Constructore
        /// </summary>
        /// <param name="logManager"></param>
        /// <param name="srcPath"></param>
        /// <param name="destPath"></param>
        /// <param name="destFolder"></param>
        public Data(LogManager logManager, string srcPath, string destPath, string destFolder, Report report, FlowState state) : base(logManager, srcPath, destPath, destFolder)
        {
            _logSection = "Data";
            _report = report;
            _state = state;
            _codeLists = new List<string>();
            _sasSpecialNumerics = new List<string>();
            _stataSpecialNumerics = new List<string>();
            foreach (char c in Alphabet)
            {
                _sasSpecialNumerics.Add(string.Format("{0}", c.ToString().ToUpper()));
                _stataSpecialNumerics.Add(string.Format(".{0}", c.ToString()));
            }
        }

        /// <summary>
        /// start converter
        /// </summary>
        /// <returns></returns>
        public override bool Run()
        {
            var result = false;
            var message = string.Format("Start Converting Data {0} -> {1}", _srcFolder, _destFolder);
            _log.Info(message);
            _logManager.Add(new LogEntity() { Level = LogLevel.Info, Section = _logSection, Message = message });
            if (EnsureMissingValues() && EnsureCodeLists() && EnsureTables())
            {
                result = true;
                if (_report.ScriptType == ScriptType.SPSS) { result = EnsureUserCodes(); }
            }
            message = result ? "End Converting Data" : "End Converting Data with errors";
            _log.Info(message);
            _logManager.Add(new LogEntity() { Level = LogLevel.Info, Section = _logSection, Message = message });
            return result;
        }

        /// <summary>
        /// flush and save report file
        /// </summary>
        /// <param name="path"></param>
        /// <param name="name"></param>
        public bool Flush(string path, string name)
        {
            var result = true;
            try
            {
                _log.Info("Flush report");
                var json = new JavaScriptSerializer().Serialize(_report);
                string data = GetReportTemplate();
                File.WriteAllText(path, string.Format(data, DateTime.Now.ToString("dd-MM-yyyy HH:mm:ss"), name, json));
            }
            catch (Exception ex)
            {
                result = false;
                _log.Error("Failed to Flush log", ex);
            }
            return result;
        }

        private bool EnsureTables()
        {
            var result = true;
            string path = null;
            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Info,
                Section = _logSection,
                Message = "Starting table processing for all tables"
            });

            try
            {
                _report.Tables.ForEach(table =>
                {
                    table.RowsCounter = 0;
                    XNamespace tableNS = string.Format(TableXmlNs, table.SrcFolder);
                    path = string.Format(TableDataPath, _destFolderPath, _report.ScriptType.ToString().ToLower(), NormalizeName(table.Name));

                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Info,
                        Section = _logSection,
                        Message = $"Processing table '{table.Name}' to path: {path}"
                    });

                    using (TextWriter sw = new StreamWriter(path))
                    {
                        _logManager.Add(new LogEntity()
                        {
                            Level = LogLevel.Info,
                            Section = _logSection,
                            Message = $"Writing header for table '{table.Folder}' with {table.Columns.Count} columns"
                        });

                        sw.WriteLine(string.Join(Separator, table.Columns.Select(c => NormalizeName(c.Name)).ToArray()));
                        path = string.Format(TablePath, _srcPath, table.SrcFolder);

                        StreamElement(delegate (XElement row) {
                            table.RowsCounter++;
                            sw.WriteLine(GetRow(table, row, tableNS));
                            if ((table.RowsCounter % RowsChunk) == 0)
                            {
                                _logManager.Add(new LogEntity()
                                {
                                    Level = LogLevel.Info,
                                    Section = _logSection,
                                    Message = $"Table '{table.Name}': Processed {table.RowsCounter:N0} of {table.Rows:N0} rows ({(double)table.RowsCounter / table.Rows:P1} complete)"
                                });
                            }
                        }, path);
                    }
                    _report.TablesCounter++;
                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Info,
                        Section = _logSection,
                        Message = $"Completed processing table '{table.Name}'. Total rows: {table.RowsCounter:N0}"
                    });
                });

                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Info,
                    Section = _logSection,
                    Message = $"Successfully processed all {_report.TablesCounter} tables"
                });
            }
            catch (Exception ex)
            {
                result = false;
                _log.Error($"Failed processing tables at path: {path}", ex);
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Error,
                    Section = _logSection,
                    Message = $"Table processing failed: {ex.Message}. Stack trace: {ex.StackTrace}"
                });
            }
            return result;
        }

        private string GetRow(Table table, XElement row, XNamespace tableNS)
        {
            var result = string.Empty;
            var contents = new List<string>();

            table.Columns.ForEach(column => {
                var hasError = false;
                var isDifferent = false;
                var content = row.Element(tableNS + column.Id).Value;
                if (!string.IsNullOrEmpty(content))
                {
                    content = GetConvertedValue(column, content, out hasError, out isDifferent, table.RowsCounter);
                }
                contents.Add(content);
            });
            return string.Join(Separator, contents.ToArray());
        }

        private void UpdateRange(Column column, string value)
        {
            if (string.IsNullOrEmpty(column.Highest)) { column.Highest = "0"; }
            if (string.IsNullOrEmpty(column.Lowest)) { column.Lowest = "0"; }

            if (column.TypeOriginal == "INTEGER")
            {
                int current = 0;
                if (int.TryParse(value, out current))
                {
                    var tmp = int.Parse(column.Highest);
                    if (current > tmp) { column.Highest = current.ToString(); }
                    tmp = int.Parse(column.Lowest);
                    if (current < tmp) { column.Lowest = current.ToString(); }
                }
            }
            if (column.TypeOriginal == "DECIMAL")
            {
                var newValue = value.Replace(",", ".");
                decimal current = 0;
                if (decimal.TryParse(newValue, out current))
                {
                    var tmp = decimal.Parse(column.Highest);
                    if (current > tmp) { column.Highest = current.ToString(); }
                    tmp = decimal.Parse(column.Lowest);
                    if (current < tmp) { column.Lowest = current.ToString(); }
                }
            }
        }

        private void EnsureTableMissingValues(Table table, XNamespace tableNS)
        {
            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Info,
                Section = _logSection,
                Message = $"Starting missing values analysis for table: {table.Name}"
            });

            var counter = 0;
            var columnsWithMissing = table.Columns.Where(c => c.MissingValues != null).ToList();

            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Info,
                Section = _logSection,
                Message = $"Found {columnsWithMissing.Count} columns with missing values to process"
            });

            var path = string.Format(TablePath, _srcPath, table.SrcFolder);

            try
            {
                StreamElement(delegate (XElement row) {
                    columnsWithMissing.ForEach(column => {
                        var content = row.Element(tableNS + column.Id).Value;
                        if (!string.IsNullOrEmpty(content))
                        {
                            UpdateRange(column, content);
                        }
                    });
                    counter++;
                    if ((counter % RowsChunk) == 0)
                    {
                        _logManager.Add(new LogEntity()
                        {
                            Level = LogLevel.Info,
                            Section = _logSection,
                            Message = $"Processed {counter:N0} of {table.Rows:N0} rows ({(double)counter / table.Rows:P1} complete)"
                        });
                    }
                }, path);

                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Info,
                    Section = _logSection,
                    Message = $"Completed missing values analysis for table {table.Name}"
                });
            }
            catch (Exception ex)
            {
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Error,
                    Section = _logSection,
                    Message = $"Error analyzing missing values for table {table.Name}: {ex.Message}"
                });
                throw;
            }
        }

        private void EnsureCodeListMissingValues(Table table)
        {
            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Info,
                Section = _logSection,
                Message = $"Starting code list missing values analysis for table: {table.Name}"
            });

            string path = null;
            var columnsWithCodeList = table.Columns.Where(c => c.CodeList != null && c.MissingValues != null).ToList();

            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Info,
                Section = _logSection,
                Message = $"Found {columnsWithCodeList.Count} columns with code lists and missing values"
            });

            try
            {
                columnsWithCodeList.ForEach(column =>
                {
                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Info,
                        Section = _logSection,
                        Message = $"Processing missing values for code list: {column.CodeList.Name}"
                    });

                    XNamespace tableNS = string.Format(TableXmlNs, column.CodeList.SrcFolder);
                    path = string.Format(TablePath, _srcPath, column.CodeList.SrcFolder);
                    var columnId = column.CodeList.Columns.Where(c => c.IsKey).Select(c => c.Id).FirstOrDefault();

                    var processedRows = 0;
                    StreamElement(delegate (XElement row) {
                        var content = row.Element(tableNS + columnId).Value;
                        UpdateRange(column, content);
                        processedRows++;

                        if (processedRows % 1000 == 0)
                        {
                            _logManager.Add(new LogEntity()
                            {
                                Level = LogLevel.Debug,
                                Section = _logSection,
                                Message = $"Processed {processedRows} rows for code list {column.CodeList.Name}"
                            });
                        }
                    }, path);

                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Info,
                        Section = _logSection,
                        Message = $"Completed missing values analysis for code list {column.CodeList.Name}"
                    });
                });
            }
            catch (Exception ex)
            {
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Error,
                    Section = _logSection,
                    Message = $"Error analyzing code list missing values at path {path}: {ex.Message}"
                });
                throw;
            }
        }


        private void ConvertMissingValuesToIntegers(Regex regex, Column column, int length, List<string> availableNumerics)
        {
            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Info,
                Section = _logSection,
                Message = $"Starting integer conversion for column {column.Name} with length {length}"
            });

            if (length > 0)
            {
                if (length > 9)
                {
                    length = 9;
                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Warning,
                        Section = _logSection,
                        Message = $"Length truncated to 9 for column {column.Name}"
                    });
                }

                int newValue = ((int.Parse(Math.Pow(10, length).ToString()) - 1) * -1);
                var matchingValues = column.MissingValues.Where(v => regex.IsMatch(v.Key)).ToList();

                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Info,
                    Section = _logSection,
                    Message = $"Found {matchingValues.Count} values to convert for column {column.Name}"
                });

                matchingValues.ForEach(value =>
                {
                    while (int.Parse(column.Lowest) > newValue && availableNumerics.Contains(newValue.ToString()))
                    {
                        newValue++;
                    }

                    if (newValue >= int.Parse(column.Lowest))
                    {
                        column.Message = $"No new numeric code less than {column.Lowest} available for column: {column.Name}";
                        _logManager.Add(new LogEntity()
                        {
                            Level = LogLevel.Warning,
                            Section = _logSection,
                            Message = column.Message
                        });
                    }
                    else
                    {
                        _logManager.Add(new LogEntity()
                        {
                            Level = LogLevel.Info,
                            Section = _logSection,
                            Message = $"Mapped column {column.Name} Missing Value {value.Key} to {newValue}"
                        });
                        column.MissingValues[value.Key] = newValue.ToString();
                        availableNumerics.Add(newValue.ToString());
                    }
                });

                if (string.IsNullOrEmpty(column.Message))
                {
                    column.SortedMissingValues = column.MissingValues.Values.ToList();
                    column.SortedMissingValues.Sort(new IntComparer());
                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Info,
                        Section = _logSection,
                        Message = $"Successfully completed integer conversion for column {column.Name}"
                    });
                }
            }
        }

        private void ConvertMissingValuesToDecimals(Regex regex, Column column, int length, List<string> availableNumerics)
        {
            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Info,
                Section = _logSection,
                Message = $"Starting decimal conversion for column {column.Name} with length {length}"
            });

            if (length > 0)
            {
                if (length > 9)
                {
                    length = 9;
                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Warning,
                        Section = _logSection,
                        Message = $"Length truncated to 9 for column {column.Name}"
                    });
                }

                decimal newValue = ((decimal.Parse(Math.Pow(10, length).ToString()) - 1) * -1);
                var matchingValues = column.MissingValues.Where(v => regex.IsMatch(v.Key)).ToList();

                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Info,
                    Section = _logSection,
                    Message = $"Found {matchingValues.Count} values to convert for column {column.Name}"
                });

                matchingValues.ForEach(value =>
                {
                    while (decimal.Parse(column.Lowest) > newValue && availableNumerics.Contains(newValue.ToString()))
                    {
                        newValue++;
                    }

                    if (newValue >= decimal.Parse(column.Lowest))
                    {
                        column.Message = $"No new decimal code less than {column.Lowest} available for column: {column.Name}";
                        _logManager.Add(new LogEntity()
                        {
                            Level = LogLevel.Warning,
                            Section = _logSection,
                            Message = column.Message
                        });
                    }
                    else
                    {
                        _logManager.Add(new LogEntity()
                        {
                            Level = LogLevel.Info,
                            Section = _logSection,
                            Message = $"Mapped column {column.Name} Missing Value {value.Key} to {newValue}"
                        });
                        column.MissingValues[value.Key] = newValue.ToString();
                        availableNumerics.Add(newValue.ToString());
                    }
                });

                if (string.IsNullOrEmpty(column.Message))
                {
                    column.SortedMissingValues = column.MissingValues.Values.ToList();
                    column.SortedMissingValues.Sort(new DecimalComparer());
                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Info,
                        Section = _logSection,
                        Message = $"Successfully completed decimal conversion for column {column.Name}"
                    });
                }
            }
        }

        private void ConvertMissingValuesToNumbers(Regex regex, Column column)
        {
            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Info,
                Section = _logSection,
                Message = $"Starting numeric conversion for column {column.Name}"
            });

            var availableNumerics = new List<string>();
            var existingValues = column.MissingValues.Where(v => !regex.IsMatch(v.Key)).ToList();

            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Debug,
                Section = _logSection,
                Message = $"Found {existingValues.Count} existing numeric values in column {column.Name}"
            });

            existingValues.ForEach(value => {
                if (!availableNumerics.Contains(value.Value))
                {
                    availableNumerics.Add(value.Value);
                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Debug,
                        Section = _logSection,
                        Message = $"Added {value.Value} to available numerics pool for column {column.Name}"
                    });
                }
            });

            int length = -1;
            if (column.TypeOriginal == "INTEGER")
            {
                length = GetIntegerLength(column);
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Info,
                    Section = _logSection,
                    Message = $"Converting to integers with length {length} for column {column.Name}"
                });
                ConvertMissingValuesToIntegers(regex, column, length, availableNumerics);
            }
            if (column.TypeOriginal == "DECIMAL")
            {
                length = GetDecimalLength(column)[0];
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Info,
                    Section = _logSection,
                    Message = $"Converting to decimals with length {length} for column {column.Name}"
                });
                ConvertMissingValuesToDecimals(regex, column, length, availableNumerics);
            }

            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Info,
                Section = _logSection,
                Message = $"Completed numeric conversion for column {column.Name}"
            });
        }

        private void ConvertMissingValuesToChars(Regex regex, Column column, string[] specialNumerics)
        {
            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Info,
                Section = _logSection,
                Message = $"Starting character conversion for column {column.Name}"
            });

            var result = true;
            var availableNumerics = new List<string>();
            availableNumerics.AddRange(specialNumerics);

            var existingValues = column.MissingValues.Where(v => regex.IsMatch(v.Value)).ToList();
            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Info,
                Section = _logSection,
                Message = $"Found {existingValues.Count} existing special numeric values"
            });

            existingValues.ForEach(value => {
                if (availableNumerics.Contains(value.Value))
                {
                    availableNumerics.Remove(value.Value);
                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Debug,
                        Section = _logSection,
                        Message = $"Removed {value.Value} from available numerics pool"
                    });
                }
            });

            var valuesToConvert = column.MissingValues.Where(v => !regex.IsMatch(v.Value)).ToList();
            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Info,
                Section = _logSection,
                Message = $"Found {valuesToConvert.Count} values to convert"
            });

            valuesToConvert.ForEach(value =>
            {
                result = availableNumerics.Count > 0;
                if (result)
                {
                    var newValue = availableNumerics[0];
                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Info,
                        Section = _logSection,
                        Message = $"Mapped column {column.Name} Missing Value {value.Key} to {newValue}"
                    });
                    column.MissingValues[value.Key] = newValue;
                    availableNumerics.Remove(newValue);
                }
            });

            if (!result)
            {
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Warning,
                    Section = _logSection,
                    Message = $"No new Special Numeric codes available for column: {column.Name}"
                });
            }
            else
            {
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Info,
                    Section = _logSection,
                    Message = $"Successfully completed character conversion for column {column.Name}"
                });
            }
        }

        private bool EnsureMissingValues()
        {
            var result = true;
            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Info,
                Section = _logSection,
                Message = $"Starting missing values processing for script type: {_report.ScriptType}"
            });

            try
            {
                Regex regex = null;
                switch (_report.ScriptType)
                {
                    case ScriptType.SPSS:
                        regex = GetRegex(SpecialNumericPattern);
                        _logManager.Add(new LogEntity()
                        {
                            Level = LogLevel.Info,
                            Section = _logSection,
                            Message = "Using SPSS special numeric pattern"
                        });
                        break;
                    case ScriptType.SAS:
                        regex = GetRegex(SasSpecialNumericPattern);
                        _logManager.Add(new LogEntity()
                        {
                            Level = LogLevel.Info,
                            Section = _logSection,
                            Message = "Using SAS special numeric pattern"
                        });
                        break;
                    case ScriptType.Stata:
                        regex = GetRegex(StataSpecialNumericPattern);
                        _logManager.Add(new LogEntity()
                        {
                            Level = LogLevel.Info,
                            Section = _logSection,
                            Message = "Using Stata special numeric pattern"
                        });
                        break;
                }

                _report.Tables.ForEach(table =>
                {
                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Info,
                        Section = _logSection,
                        Message = $"Processing missing values for table: {table.Name}"
                    });

                    if (_report.ScriptType == ScriptType.SPSS)
                    {
                        EnsureTableMissingValues(table, string.Format(TableXmlNs, table.SrcFolder));
                        EnsureCodeListMissingValues(table);
                    }

                    var columnsWithMissingValues = table.Columns.Where(c => c.MissingValues != null).ToList();
                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Info,
                        Section = _logSection,
                        Message = $"Found {columnsWithMissingValues.Count} columns with missing values in table {table.Name}"
                    });

                    columnsWithMissingValues.ForEach(column =>
                    {
                        _logManager.Add(new LogEntity()
                        {
                            Level = LogLevel.Info,
                            Section = _logSection,
                            Message = $"Processing missing values for column {column.Name} in table {table.Name}"
                        });

                        if (_report.ScriptType == ScriptType.SPSS)
                        {
                            ConvertMissingValuesToNumbers(regex, column);
                        }
                        if (_report.ScriptType == ScriptType.SAS)
                        {
                            ConvertMissingValuesToChars(regex, column, _sasSpecialNumerics.ToArray());
                        }
                        if (_report.ScriptType == ScriptType.Stata)
                        {
                            ConvertMissingValuesToChars(regex, column, _stataSpecialNumerics.ToArray());
                        }
                    });
                });

                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Info,
                    Section = _logSection,
                    Message = "Successfully completed missing values processing for all tables"
                });
            }
            catch (Exception ex)
            {
                result = false;
                _log.Error("Missing values processing failed", ex);
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Error,
                    Section = _logSection,
                    Message = $"Missing values processing failed: {ex.Message}. Stack trace: {ex.StackTrace}"
                });
            }
            return result;
        }

        private bool EnsureUserCodes()
        {
            var result = true;
            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Info,
                Section = _logSection,
                Message = "Starting user codes generation for all tables"
            });

            try
            {
                var tablesWithUserCodes = _report.Tables.Where(t => t.Columns.Any(c => c.MissingValues != null)).ToList();

                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Info,
                    Section = _logSection,
                    Message = $"Found {tablesWithUserCodes.Count} tables requiring user codes"
                });

                tablesWithUserCodes.ForEach(table =>
                {
                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Info,
                        Section = _logSection,
                        Message = $"Generating user codes for table: {table.Name}"
                    });

                    EnsureUserCode(table);

                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Info,
                        Section = _logSection,
                        Message = $"Completed user codes generation for table: {table.Name}"
                    });
                });

                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Info,
                    Section = _logSection,
                    Message = "Successfully completed user codes generation for all tables"
                });
            }
            catch (Exception ex)
            {
                result = false;
                _log.Error("EnsureUserCodes Failed", ex);
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Error,
                    Section = _logSection,
                    Message = $"User codes generation failed: {ex.Message}"
                });
            }
            return result;
        }

        private void EnsureUserCode(Table table)
        {
            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Info,
                Section = _logSection,
                Message = $"Starting user code generation for table {table.Name}"
            });

            string content = null;
            var usercodes = new List<string>();
            var columnsWithMissingValues = table.Columns.Where(c => c.MissingValues != null).ToList();

            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Info,
                Section = _logSection,
                Message = $"Processing {columnsWithMissingValues.Count} columns with missing values"
            });

            columnsWithMissingValues.ForEach(column =>
            {
                content = string.Empty;
                if (column.SortedMissingValues != null)
                {
                    if (column.SortedMissingValues.Count < 4)
                    {
                        content = string.Join(" ", column.SortedMissingValues.Select(v => string.Format("'{0}'", v)).ToArray());
                        _logManager.Add(new LogEntity()
                        {
                            Level = LogLevel.Debug,
                            Section = _logSection,
                            Message = $"Generated simple user codes for column {column.Name}: {content}"
                        });
                    }
                    else
                    {
                        _logManager.Add(new LogEntity()
                        {
                            Level = LogLevel.Info,
                            Section = _logSection,
                            Message = $"Applying user codes range for column {column.Name}"
                        });

                        string lastValue = column.SortedMissingValues[0];
                        var lastIndex = 1;
                        column.SortedMissingValues.ForEach(v => {
                            if ((column.TypeOriginal == "INTEGER" && int.Parse(v) == (int.Parse(lastValue) + 1)) ||
                                (column.TypeOriginal == "DECIMAL" && decimal.Parse(v) == (decimal.Parse(lastValue) + 1)))
                            {
                                lastValue = v;
                                lastIndex++;
                            }
                        });

                        if (column.SortedMissingValues.Count > (lastIndex + 1))
                        {
                            column.Message = "out of range";
                            _logManager.Add(new LogEntity()
                            {
                                Level = LogLevel.Warning,
                                Section = _logSection,
                                Message = $"Values out of range detected for column {column.Name}"
                            });
                        }

                        content = string.Format(UserCodeRange,
                            column.SortedMissingValues[0],
                            lastValue,
                            lastIndex < column.SortedMissingValues.Count ?
                                string.Format(UserCodeExtra, column.SortedMissingValues[lastIndex]) :
                                string.Empty);

                        _logManager.Add(new LogEntity()
                        {
                            Level = LogLevel.Debug,
                            Section = _logSection,
                            Message = $"Generated range-based user codes for column {column.Name}"
                        });
                    }
                }
                usercodes.Add(content);
            });

            if (usercodes.Count == 0)
            {
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Info,
                    Section = _logSection,
                    Message = $"No user codes to generate for table {table.Name}"
                });
                return;
            }

            try
            {
                var path = string.Format(UserCodesPath, _destFolderPath, _report.ScriptType.ToString().ToLower(), NormalizeName(table.Name));
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Info,
                    Section = _logSection,
                    Message = $"Writing user codes to file: {path}"
                });

                content = File.ReadAllText(path);
                using (var sw = new StreamWriter(path, false, Encoding.UTF8))
                {
                    sw.Write(string.Format(content, usercodes.ToArray()));
                }

                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Info,
                    Section = _logSection,
                    Message = $"Successfully wrote user codes for table {table.Name}"
                });
            }
            catch (Exception ex)
            {
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Error,
                    Section = _logSection,
                    Message = $"Failed to write user codes for table {table.Name}: {ex.Message}"
                });
                throw;
            }
        }

        private bool EnsureCodeLists()
        {
            var result = true;
            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Info,
                Section = _logSection,
                Message = "Starting code lists processing"
            });

            try
            {
                _report.Tables.ForEach(table =>
                {
                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Info,
                        Section = _logSection,
                        Message = $"Processing code lists for table: {table.Name}"
                    });

                    _codeLists.Clear();
                    var columnsWithCodeList = table.Columns.Where(c => c.CodeList != null).ToList();

                    if (columnsWithCodeList.Any())
                    {
                        _logManager.Add(new LogEntity()
                        {
                            Level = LogLevel.Info,
                            Section = _logSection,
                            Message = $"Found {columnsWithCodeList.Count} columns with code lists in table {table.Name}"
                        });
                        EnsureCodeList(table);
                    }
                    else
                    {
                        _logManager.Add(new LogEntity()
                        {
                            Level = LogLevel.Info,
                            Section = _logSection,
                            Message = $"No code lists found for table {table.Name}"
                        });
                    }
                });

                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Info,
                    Section = _logSection,
                    Message = $"Successfully processed code lists for all tables. Total code lists processed: {_report.CodeListsCounter}"
                });
            }
            catch (Exception ex)
            {
                result = false;
                _log.Error("Code lists processing failed", ex);
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Error,
                    Section = _logSection,
                    Message = $"Code lists processing failed: {ex.Message}. Stack trace: {ex.StackTrace}"
                });
            }
            return result;
        }

        private void EnsureCodeList(Table table)
        {
            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Info,
                Section = _logSection,
                Message = $"Starting code list generation for table {table.Name}"
            });

            string path = null;
            var codeList = new StringBuilder();
            var columnsWithCodeList = table.Columns.Where(c => c.CodeList != null).ToList();

            _logManager.Add(new LogEntity()
            {
                Level = LogLevel.Info,
                Section = _logSection,
                Message = $"Found {columnsWithCodeList.Count} columns with code lists in table {table.Name}"
            });

            try
            {
                columnsWithCodeList.ForEach(column =>
                {
                    column.CodeList.RowsCounter = 0;
                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Info,
                        Section = _logSection,
                        Message = $"Processing code list for column: {column.Name} ({column.CodeList.Name})"
                    });

                    XNamespace tableNS = string.Format(TableXmlNs, column.CodeList.SrcFolder);
                    path = string.Format(TablePath, _srcPath, column.CodeList.SrcFolder);
                    var columnKeyId = column.CodeList.Columns.Where(c => c.IsKey).Select(c => c.Id).FirstOrDefault();
                    var columnId = column.CodeList.Columns.Where(c => !c.IsKey).Select(c => c.Id).FirstOrDefault();

                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Debug,
                        Section = _logSection,
                        Message = $"Reading code list from path: {path}"
                    });

                    StreamElement(delegate (XElement row) {
                        var code = row.Element(tableNS + columnKeyId).Value;
                        if (column.MissingValues != null && column.MissingValues.ContainsKey(code))
                        {
                            code = column.MissingValues[code];
                            _logManager.Add(new LogEntity()
                            {
                                Level = LogLevel.Debug,
                                Section = _logSection,
                                Message = $"Mapped missing value code: {code} in column {column.Name}"
                            });
                        }

                        var codeDescription = row.Element(tableNS + columnId).Value;
                        codeList.AppendLine(string.Format(CodeFormat, code, EnsureNewLines(codeDescription)));
                        CheckCodeListDescriptionLength(column, codeDescription, code);
                        column.CodeList.RowsCounter++;

                        if (column.CodeList.RowsCounter % 1000 == 0)
                        {
                            _logManager.Add(new LogEntity()
                            {
                                Level = LogLevel.Info,
                                Section = _logSection,
                                Message = $"Processed {column.CodeList.RowsCounter} codes for {column.CodeList.Name}"
                            });
                        }
                    }, path);

                    var codeListContent = codeList.ToString();
                    _codeLists.Add(codeListContent.Substring(0, codeListContent.Length - 2));
                    codeList.Clear();
                    _report.CodeListsCounter++;

                    _logManager.Add(new LogEntity()
                    {
                        Level = LogLevel.Info,
                        Section = _logSection,
                        Message = $"Completed processing code list for {column.Name}. Total codes: {column.CodeList.RowsCounter}"
                    });
                });

                // Skriv code lists til fil
                path = string.Format(CodeListPath, _destFolderPath, _report.ScriptType.ToString().ToLower(), NormalizeName(table.Name));
                var content = File.ReadAllText(path);

                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Info,
                    Section = _logSection,
                    Message = $"Writing code lists to file: {path}"
                });

                using (var sw = new StreamWriter(path, false, Encoding.UTF8))
                {
                    sw.Write(string.Format(content, _codeLists.ToArray()));
                }

                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Info,
                    Section = _logSection,
                    Message = $"Successfully wrote all code lists for table {table.Name}"
                });
            }
            catch (Exception ex)
            {
                _logManager.Add(new LogEntity()
                {
                    Level = LogLevel.Error,
                    Section = _logSection,
                    Message = $"Failed processing code list at path {path}: {ex.Message}"
                });
                throw;
            }
        }


        private void CheckCodeListDescriptionLength(Column column, string codeDescription, string code)
        {
            var codeDescriptionLength = Encoding.UTF8.GetByteCount(codeDescription);
            if (codeDescriptionLength > CodeDescriptionMaxLength)
            {
                _logManager.Add(new LogEntity() { Level = LogLevel.Warning, Section = _logSection, Message = $"Value label: {column.Name} ('{code}') has been truncated" });
                column.CodeDescriptionLengthExceededList.Add(new CodeDescriptionLengthExceeded { ByteLength = codeDescriptionLength, Code = code });
            }
        }

        private string GetReportTemplate()
        {
            string result = null;
            using (Stream stream = _assembly.GetManifestResourceStream(ResourceReportFile))
            {
                using (StreamReader reader = new StreamReader(stream, Encoding.UTF8))
                {
                    result = reader.ReadToEnd();
                }
            }
            return result;
        }
    }
}
