using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;

namespace DbfTests
{
    internal class DbfFileMerger
    {
        public IList<OutputRow> MergeFiles(string sourceDirectory, string fileName)
        {
            var dbfFiles = Directory.EnumerateFiles(sourceDirectory, fileName, SearchOption.AllDirectories);

            var sortedOutputRows = new SortedList<DateTime, OutputRow>();
            var numberOfFiles = dbfFiles.Count();
            var currentFile = 0;

            foreach (var dbfFile in dbfFiles)
            {
                var directoryName = Path.GetDirectoryName(dbfFile);
                OutputRow.Headers.Add(directoryName);
                var valueRows = this.ReadDbfFile(dbfFile);
                
                foreach (var valueRow in valueRows)
                {
                    if (sortedOutputRows.TryGetValue(valueRow.Timestamp, out var outputRow))
                    {
                        outputRow.Values[currentFile] = valueRow.Value;
                    }
                    else
                    {
                        var newOutputRow = new OutputRow
                        {
                            Timestamp = valueRow.Timestamp,
                            Values = new List<double?>(numberOfFiles)
                        };
                        for (int i = 0; i < numberOfFiles; i++)
                        {
                            newOutputRow.Values.Add(null);
                        }
                        newOutputRow.Values[currentFile] = valueRow.Value;

                        sortedOutputRows.Add(valueRow.Timestamp, newOutputRow);
                    }
                }
                currentFile++;
            }

            return sortedOutputRows.Values;
        }

        private IList<DbfReader.ValueRow> ReadDbfFile(string dbfFilePath)
        {
            var reader = new DbfReader();
            return reader.ReadValues(dbfFilePath);
        }
    }
}