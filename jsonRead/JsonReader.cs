using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using System.IO;
namespace jsonRead {
    class JsonReader {
        private List<string> connList = new List<string>();

        public JsonReader() {
            JObject c = (JObject)JsonConvert.DeserializeObject(File.ReadAllText("conn.json"));
            foreach (var item in c) {
                var p = item.Value.ToString();
                connList.Add(p);
            }
        }

        //正则表达式验证是否为数字
        public bool IsNumeric(string value) {
            return Regex.IsMatch(value, @"^[+-]?\d*[.]?\d*$");
        }

        /// <summary>
        /// 批量储存json
        /// </summary>
        /// <param name="sourcePath">json文件夹路径</param>
        /// <param name="connId">选择数据库连接字符串(从1开始)</param>
        public void BatchStorage(string sourcePath,int connId) {
            var fileList = ScanningFile(sourcePath);
            foreach (var a in fileList) {
                string[] arr = Regex.Split(a, "\\\\", RegexOptions.IgnoreCase);
                string[] rs = Regex.Split(arr[arr.Length - 1], ".json", RegexOptions.IgnoreCase);
                JsonToSql(a, rs[0], connList[connId-1]);
            }
            Console.ReadKey();
        }

        //扫描文件目录
        public List<string> ScanningFile(string sourcePath) {
            List<string> list = new List<string>();
            //遍历文件夹
            DirectoryInfo folder = new DirectoryInfo(sourcePath);
            FileInfo[] theFileInfo = folder.GetFiles("*.json", SearchOption.TopDirectoryOnly);
            foreach (FileInfo nextFile in theFileInfo) { //遍历文件
                list.Add(nextFile.FullName);
            }

            //遍历子文件夹
            DirectoryInfo[] dirInfo = folder.GetDirectories();
            foreach (DirectoryInfo nextFolder in dirInfo) {
                FileInfo[] fileInfo = nextFolder.GetFiles("*.json", SearchOption.AllDirectories);
                foreach (FileInfo nextFile in fileInfo) { //遍历文件
                    list.Add(nextFile.FullName);
                }
            }
            return list;
        }

        //验证数据库字段
        public string validateFieldName(string itemKey) {
            if (itemKey == "key")
                itemKey = "sKey";
            if (itemKey == "id")
                itemKey = "sId";
            return itemKey;
        }

        //检查是否有bool值
        public int BoolConverter(JToken sender) {
            if (sender.ToString().ToLower() == "true")
                return 1;
            //else if (sender.ToString() == "false" || sender.ToString() == "FALSE")
            //    return 0;
            else
                return 0;
        }

        //获取需要创建的字段类型
        public string GetFieldType(JTokenType curType) {
            switch (curType) {
                case JTokenType.Integer:
                    return "int";
                    break;
                case JTokenType.String:
                    return "text";
                    break;
                case JTokenType.Boolean:
                    return "bit";
                    break;
                case JTokenType.Float:
                    return "float";
                    break;
                default:
                    return "text";
                    break;
            }
        }

        public void MsgSendTypeNormal(string conn, string fname, string mainKey, string itemKey, string item, string sendType) {
            string sql = string.Format("call procCreateAndInsertField({0},{1},{2},{3},{4})", fname, mainKey, "\"" + itemKey + "\"", "\"'" + item + "'\"", "\"" + sendType + "\"");
            MySqlHelper.ExecuteNonQuery(conn, System.Data.CommandType.Text, sql, null);
        }

        public void MsgSendTypeBool(string conn, string fname, string mainKey, string itemKey, int boolItem, string sendType) {
            string sql = string.Format("call procCreateAndInsertField({0},{1},{2},{3},{4})", fname, mainKey, "\"" + itemKey + "\"", boolItem, "\"" + sendType + "\"");
            MySqlHelper.ExecuteNonQuery(conn, System.Data.CommandType.Text, sql, null);
        }

        public void MsgSendTypeNum(string conn, string fname, string mainKey, string itemKey, string item, string sendType) {
            string sql = string.Format("call procCreateAndInsertField({0},{1},{2},{3},{4})", fname, mainKey, "'`" + itemKey + "`'", "\"'" + item + "'\"", "\"" + sendType + "\"");
            MySqlHelper.ExecuteNonQuery(conn, System.Data.CommandType.Text, sql, null);
        }


        /// <summary>
        /// 处理嵌套obj
        /// </summary>
        /// <param name="conn">数据库连接字符串</param>
        /// <param name="p">当前obj</param>
        /// <param name="name">数据库表名</param>
        /// <param name="mainKey">字段主键</param>
        /// <returns></returns>
        public string DuelObjType(string conn, JToken p, string name, string mainKey) {
            //嵌套的key和value
            string fname = "\"" + name + "\"";
            string itemKey = ((JProperty)p).Name;
            itemKey = validateFieldName(itemKey);
            var item = p.First.ToString();
            JTokenType curType = p.First.Type;
            if (curType == JTokenType.Object) {
                //将子KEY作为item保存
                item = DuelObjType(conn, p.First.First, name, mainKey);
            }
            string sendType = GetFieldType(curType);
            //bool值字段
            if (sendType == "bit") {
                int boolItem = BoolConverter(p.First);
                MsgSendTypeBool(conn, fname, mainKey, itemKey, boolItem, sendType);
                return itemKey;
            }
            //检测KEY是否为纯数字
            if (IsNumeric(itemKey) == true) {
                //检测纯数字字段是否重复
                string sql = string.Format("SELECT * FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = {0} AND column_name = '{1}'", fname, itemKey);
                var rs = MySqlHelper.ExecuteScalar(MySqlHelper.Conn, System.Data.CommandType.Text, sql, null);
                if (rs != null) {
                    var s = string.Format("UPDATE {0} set `{1}` = '{2}' where MainKey = {3}", name, itemKey, item, mainKey);
                    MySqlHelper.ExecuteNonQuery(MySqlHelper.Conn, System.Data.CommandType.Text, s, null);
                    return itemKey;
                }
                MsgSendTypeNum(conn, fname, mainKey, itemKey, item, sendType);
                return itemKey;
            }
            MsgSendTypeNormal(conn, fname, mainKey, itemKey, item, sendType);
            return itemKey;
        }

        /// <summary>
        /// 连接数据库储存json
        /// </summary>
        /// <param name="path">json文件路径</param>
        /// <param name="name">对应数据库表名</param>
        /// <param name="conn">数据库连接字符串</param>
        public void JsonToSql(string path, string name, string conn) {
            //反序列化json
            try {
                string fname = "\"" + name + "\"";
                JObject a = (JObject)JsonConvert.DeserializeObject(File.ReadAllText(path));
                //按name新建表，若已存在则不建
                //string sql = string.Format("call procCreateTable({0})", fname);
                string sql = string.Format("create table if not exists {0} (id bigint(20) primary key NOT NULL AUTO_INCREMENT,MainKey text); ", name);
                MySqlHelper.ExecuteNonQuery(conn, System.Data.CommandType.Text, sql, null);
                //遍历json

                foreach (var items in a) {
                    // string sql1 = string.Format("call procInitKey({0},{1})", fname, "\'" + items.Key + "\'");
                    string mainKey = "\'" + items.Key + "\'";
                    string sql1 = string.Format("insert into {0} (MainKey) select {1} from DUAL where not exists(select MainKey from {0} where MainKey = {1}) limit 1", name, mainKey);
                    MySqlHelper.ExecuteNonQuery(conn, System.Data.CommandType.Text, sql1, null);
                    var p = items.Value.First;
                    while (p != null) {
                        //嵌套的key和value
                        string itemKey = ((JProperty)p).Name;
                        itemKey = validateFieldName(itemKey);
                        var item = p.First.ToString();
                        JTokenType curType = p.First.Type;
                        if (curType == JTokenType.Object) {
                            item = DuelObjType(conn, p.First.First, name, mainKey);
                            //break;
                        }
                        string sendType = GetFieldType(curType);
                        if (sendType == "bit") {
                            int boolItem = BoolConverter(p.First);
                            MsgSendTypeBool(conn, fname, mainKey, itemKey, boolItem, sendType);
                            p = p.Next;
                            break;
                        }
                        MsgSendTypeNormal(conn, fname, mainKey, itemKey, item, sendType);
                        p = p.Next;
                    }
                }
                Console.WriteLine("success");
            }
            catch (Exception e) {
                Console.WriteLine(e);
            }
        }
    }
}
