﻿using Rigsarkiv.Asta.Logging;
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
    /// Data converter
    /// </summary>
    public class Data : Converter
    {
        const int RowsChunk = 500;
        const string Separator = ";";
        const string TablePath = "{0}\\Tables\\{1}\\{1}.xml";
        const string CodeFormat = "'{0}' '{1}'"; 
        private List<string> _codeLists = null;        

        /// <summary>
        /// Constructore
        /// </summary>
        /// <param name="logManager"></param>
        /// <param name="srcPath"></param>
        /// <param name="destPath"></param>
        /// <param name="destFolder"></param>
        public Data(LogManager logManager, string srcPath, string destPath, string destFolder, Report report) : base(logManager, srcPath, destPath, destFolder)
        {
            _logSection = "Data";
            _report = report;
            _codeLists = new List<string>();
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
            if(EnsureCodeLists() && EnsureTables())
            {
                result = true;
            }
            message = result ? "End Converting Data" : "End Converting Data with errors";
            _log.Info(message);
            _logManager.Add(new LogEntity() { Level = LogLevel.Info, Section = _logSection, Message = message });
            return result;
        }

        private bool EnsureTables()
        {
            var result = true;
            string path = null;
            try
            {
                 _report.Tables.ForEach(table =>
                {
                    var counter = 0;
                    XNamespace tableNS = string.Format(TableXmlNs, table.SrcFolder);
                    path = string.Format(TableDataPath, _destFolderPath, table.Folder, table.Name);
                    _logManager.Add(new LogEntity() { Level = LogLevel.Info, Section = _logSection, Message = string.Format("Add file: {0}", path) });
                    using (TextWriter sw = new StreamWriter(path))
                    {
                        _logManager.Add(new LogEntity() { Level = LogLevel.Info, Section = _logSection, Message = string.Format("Write {0} data header", table.Folder) });
                        sw.WriteLine(string.Join(Separator, table.Columns.Select(c => c.Name).ToArray()));
                        counter++;
                        path = string.Format(TablePath, _srcPath, table.SrcFolder);
                        StreamElement(delegate (XElement row) {
                            sw.WriteLine(GetRow(table, row, tableNS));
                            counter++;
                            if ((counter % RowsChunk) == 0) { _logManager.Add(new LogEntity() { Level = LogLevel.Info, Section = _logSection, Message = string.Format("{0} of {1} rows added", counter, table.Rows) }); }
                        }, path);
                    }
                });
            }
            catch (Exception ex)
            {
                result = false;
                _log.Error("EnsureTables Failed", ex);
                _logManager.Add(new LogEntity() { Level = LogLevel.Error, Section = _logSection, Message = string.Format("EnsureTables Failed: {0}", ex.Message) });
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
                    content = GetConvertedValue(column, content, out hasError, out isDifferent);
                }
                contents.Add(content);
            });            
            return string.Join(Separator, contents.ToArray());
        }

        private bool EnsureCodeLists()
        {
            var result = true;
            try
            {
                _report.Tables.ForEach(table =>
                {
                    _logManager.Add(new LogEntity() { Level = LogLevel.Info, Section = _logSection, Message = string.Format("Write {0} code lists", table.Folder) });
                    _codeLists.Clear();
                    if (table.Columns.Any(c => c.CodeList != null))
                    {
                        EnsureCodeList(table);
                    }
                });
            }
            catch (Exception ex)
            {
                result = false;
                _log.Error("EnsureCodeLists Failed", ex);
                _logManager.Add(new LogEntity() { Level = LogLevel.Error, Section = _logSection, Message = string.Format("EnsureCodeLists Failed: {0}", ex.Message) });
            }
            return result;
        }

        private void EnsureCodeList(Table table)
        {
            string path = null;            
            var codeList = new StringBuilder();
            table.Columns.Where(c => c.CodeList != null).ToList().ForEach(column =>
            {
                _logManager.Add(new LogEntity() { Level = LogLevel.Info, Section = _logSection, Message = string.Format("Add {0} code list", column.CodeList.Name) });
                XNamespace tableNS = string.Format(TableXmlNs, column.CodeList.SrcFolder);
                path = string.Format(TablePath, _srcPath, column.CodeList.SrcFolder);
                StreamElement(delegate (XElement row) {
                    codeList.AppendLine(string.Format(CodeFormat, row.Element(tableNS + C1).Value, row.Element(tableNS + C2).Value));
                }, path);
                var codeListContent = codeList.ToString();                
                _codeLists.Add(codeListContent.Substring(0, codeListContent.Length - 2));
                codeList.Clear();
            });
            path = string.Format(CodeListPath, _destFolderPath, table.Folder, table.Name);
            var content = File.ReadAllText(path);
            using (var sw = new StreamWriter(path, false, Encoding.UTF8))
            {
                sw.Write(string.Format(content, _codeLists.ToArray()));
            }
        }
    }
}