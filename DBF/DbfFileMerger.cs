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
            var dbfReader = new DbfReader();

            foreach (var dbfFile in dbfFiles)
            {
                OutputRow.Headers.Add(Path.GetDirectoryName(dbfFile));
                
                foreach (var valueRow in dbfReader.ReadValues(dbfFile))
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
                            Values = new List<double?>(new double?[numberOfFiles]) { [currentFile] = valueRow.Value }
                        };

                        sortedOutputRows.Add(valueRow.Timestamp, newOutputRow);
                    }
                }
                currentFile++;
            }

            return sortedOutputRows.Values;
        }
    }
}