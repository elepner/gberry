using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Npgsql;

namespace Gberry.Import
{
    class Program
    {
        private const string FileName = @"C:\Users\edle\Desktop\worldcitiespop.txt";

        static void Main(string[] args)
        {
            using (var streamReader = new StreamReader(new FileStream(FileName, FileMode.Open), Encoding.UTF8))
            {
                var lines = ToLines(streamReader).Skip(1);
                int count = 0;
                using (var connection = new NpgsqlConnection("User ID=docker;Password=docker;Host=localhost;Port=5432;Database=gis;Pooling=true;"))
                {
                    connection.Open();
                    var command = connection.CreateCommand();
                    foreach (var line in lines)
                    {
                        var cells = line.Split(',');
                        if (cells.Length != 7)
                        {
                            Console.WriteLine($"Invalid line: {line}");
                        }
                        if (string.IsNullOrWhiteSpace(cells[4]))
                        {
                            continue;
                        }
                        var query = "insert into cities (country, name, display_name, region, population, shape)" +
                                    string.Format("values ('{0}','{1}','{2}','{3}',{4},ST_GeomFromText('POINT({6} {5})', 4326))", cells);

                        command.CommandText = query;
                        int affected = command.ExecuteNonQuery();
                        Debug.Assert(affected == 1, "affected == 1");
                        if (count % 100 == 0)
                        {
                            Console.WriteLine($"Inserted ${count}");
                        }
                        count++;
                    }
                }

                

                //var line = lines.First();
                
                //File.WriteAllLines(@"C:\Users\edle\Desktop\citiesutf8.txt", lines);
            }
            

            Console.WriteLine("Hello World!");
        }

        static IEnumerable<string> ToLines(StreamReader streamReader)
        {
            while (streamReader.Peek() >= 0)
            {
                yield return streamReader.ReadLine();
            }
            
        }
    }
}