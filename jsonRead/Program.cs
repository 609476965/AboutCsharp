using System;
using System.Collections.Generic;
using System.Collections;
using System.Linq;
using System.Text;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using System.IO;

namespace jsonRead {
    class Program {
        static void Main(string[] args) {
            JsonReader jr = new JsonReader();
            jr.BatchStorage("D:/SVN/server/bin/config",1);
            //JsonToSql("hero.json", "hero");
        }
    }
}
