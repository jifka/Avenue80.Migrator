using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.IO;
using System.Reflection;
using System.Configuration;
using System.Data;
using System.Data.Common;

namespace Avenue80.Migrator
{
    class Program
    {
        static void Main(string[] args)
        {
            DirectoryInfo d = new DirectoryInfo(AppDomain.CurrentDomain.BaseDirectory);
            DirectoryInfo md = new DirectoryInfo(d.FullName + "\\Migrations");
            if (args.Length == 0)
            {
                Console.WriteLine("USAGE: Migrator.exe <target> [[to:<version>|tag:<tag>]|[up[:<steps>]|down[:<steps>]]] [trace:<true|false>]");
                return;
            }

            string target = args[0];
            ConnectionStringSettings cstr = ConfigurationManager.ConnectionStrings[target.Trim()]; 
            string resolvedTarget = (cstr == null)?null:cstr.ConnectionString;
            if (resolvedTarget == null)
            {
                Console.WriteLine("\"" + target + "\" is not a valid target.");
                return;
            }

            string version = null;
            string tag = null;
            string provider = null;
            bool trace = false;

            for (int i = 1; i < args.Length; i++)
            {
                string[] parts = args[i].Split(':');
                if (parts.Length != 2 && parts[0] != "up" && parts[0] != "down" && parts[0] != "trace")
                {
                    Console.WriteLine("Invalid argument: \"" + args[i] + "\"");
                    return;
                }

                // parse specific arguments
                switch (parts[0])
                {
                    case "to":
                        version = parts[1];
                        break;
                    case "tag":
                        tag = parts[1];
                        break;
                    case "p":
                        provider = parts[1];
                        break;
                    case "trace":
                        trace = true;
                        break;
                    case "up":
                        version = "+";
                        if (parts.Length > 1) version += parts[1].Trim();
                        break;
                    case "down":
                        version = "-";
                        if (parts.Length > 1) version += parts[1].Trim();
                        break;
                    default:
                        Console.WriteLine("Invalid command: \"" + parts[0] + ":\"");
                        return;
                }
                if (tag != null)
                {
                    version = null;
                }
                
            }

            // TODO: resolve other providers
            if (provider == null)
                provider = MigSharp.ProviderNames.SqlServer2008;

            System.IO.FileStream fs = null;
            if (trace)
            {
                string logfile = md.FullName + "\\" + DateTime.Now.ToString("yyyyMMddhhmmss") + ".log";
                if (File.Exists(logfile)) File.Delete(logfile);
                fs = new FileStream(logfile, FileMode.CreateNew);
                System.Diagnostics.TextWriterTraceListener tl = new System.Diagnostics.TextWriterTraceListener(fs);
                System.Diagnostics.Trace.AutoFlush = true;
                System.Diagnostics.Trace.Listeners.Add(tl);
            }

            int cnt = 0;
            if (md.Exists)
            {
                string iMigration = typeof(MigSharp.IMigration).FullName;
                string iRMigration = typeof(MigSharp.IReversibleMigration).FullName;

                foreach (FileInfo assem in md.GetFiles("*.dll"))
                {
                    Assembly a = Assembly.LoadFile(assem.FullName);
                    
                    List<MigrationMeta> migs = new List<MigrationMeta>();
                    foreach (Type t in a.GetTypes())
                    {
                        if (t.GetInterface(iMigration) != null || t.GetInterface(iRMigration) != null)
                        {
                            MigrationMeta nm = new MigrationMeta();
                            nm.migrationClass = t.FullName;
                            nm.version = Int64.Parse(t.Name.Replace("Migration", ""));

                            object[] attr = t.GetCustomAttributes(true);
                            foreach (object cAttr in attr)
                            {
                                if (cAttr is MigSharp.MigrationExportAttribute)
                                {
                                    MigSharp.MigrationExportAttribute mEa = (MigSharp.MigrationExportAttribute)cAttr;
                                    nm.tag = mEa.Tag;
                                    nm.module = mEa.ModuleName;
                                    break;
                                }
                            }
                            migs.Add(nm);
                        }
                    }
                    if (migs.Count > 0)
                    {
                        migs.Sort();
                    }

                    // resolve version from Tag attribute
                    if (tag != null && version == null)
                    {
                        foreach (MigrationMeta mm in migs)
                        {
                            if (mm.tag != null && mm.tag == tag)
                            {
                                version = mm.version.ToString();
                                break;
                            }
                        }
                        if (version == null && tag != null)
                        {
                            Console.WriteLine("\"" + tag + "\" is not a valid tag.");
                            return;
                        }
                    }
                    
                    // resolve version for incrementals
                    if (version != null && (version.StartsWith("+") || version.StartsWith("-")))
                    {
                        long current = 0;
                        int dir = (version.Substring(0, 1) == "+") ? 1 : -1;
                        version = (version.Remove(0, 1) == "") ? "1" : version.Remove(0, 1);
                        int offset = int.Parse(version) * dir;
                        version = null;
                        DbConnection dbc = null;
                        try
                        {
                            // load version table and determine current version
                            DbProviderFactory dbp = DbProviderFactories.GetFactory("System.Data.SqlClient");
                            dbc = dbp.CreateConnection();
                            dbc.ConnectionString = resolvedTarget;
                            dbc.Open();

                            DbCommand cmd = dbc.CreateCommand();
                            cmd.CommandText = "select 1 from sys.tables where name='MigSharp'";
                            object res = cmd.ExecuteScalar();
                            if (res != null)
                            {
                                cmd.CommandText = "select top 1 timestamp, module, tag from MigSharp order by timestamp desc";
                                DbDataReader dr = cmd.ExecuteReader();
                                while (dr.Read())
                                {
                                    current = dr.GetInt64(0);
                                }
                                dbc.Close();
                            }
                        }
                        catch (Exception exp)
                        {
                            if (dbc != null && dbc.State != ConnectionState.Closed) dbc.Close();
                            LogMsg(exp,"Could not gather version info from target db!");
                            return;
                        }

                        int idx = offset -1;
                        if (current > 0)
                        {
                            foreach (MigrationMeta mm in migs)
                            {
                                idx++;
                                if (mm.version == current) break;
                            }
                        }
                        if (idx < 0)
                            version = "0";
                        else
                        {
                            if (idx > migs.Count - 1)
                                idx = migs.Count - 1;
                            version = migs[idx].version.ToString();
                        }
                    }

                    // Start migration
                    Console.WriteLine("Starting Migration target:" + target + " version:" + (string)((version == null)?"latest":version) + " trace:" + trace.ToString() + " ...");
                    MigSharp.Migrator m = null;
                    MigSharp.MigrationOptions.SetSqlTraceLevel(System.Diagnostics.SourceLevels.All);
                    m = new MigSharp.Migrator(resolvedTarget, provider);

                    try
                    {
                        if (version == null)
                        {
                            m.MigrateAll(a);
                        }
                        else
                        {
                            m.MigrateTo(a, Convert.ToInt64(version));
                        }
                    }
                    catch (Exception exp)
                    {
                        LogMsg(exp, null);
                        return;
                    }
                    cnt++;

                    if (fs != null && trace)
                    {
                        System.Diagnostics.Trace.Flush();
                        fs.Close();
                    }
                }
            }
            if (cnt == 0)
            {
                Console.WriteLine("No valid Migrations found.");
            }
            else
            {
                Console.WriteLine("Migrations completed.");
            }
        }

        private static void LogMsg(string msg)
        {
            System.Diagnostics.Trace.WriteLine(msg);
            System.Diagnostics.Trace.Flush();
            Console.WriteLine(msg);
        }
        private static void LogMsg(Exception exp, string msg)
        {
            string emsg = exp.Message + "\n" + exp.StackTrace;
            if (msg != null && msg.Trim() != "")
            {
                emsg = msg + "\n" + emsg;
            }
            System.Diagnostics.Trace.WriteLine(emsg, "Error");
            System.Diagnostics.Trace.Flush();
            Console.WriteLine(emsg);
        }
    }

    internal class MigrationMeta : IComparable<MigrationMeta>
    {
        public long version;
        public string module;
        public string tag;
        public string migrationClass;

        public int CompareTo(MigrationMeta m)
        {
            return this.version.CompareTo(m.version);
        }
    }

}
