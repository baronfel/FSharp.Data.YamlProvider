module FSharp.Data.YamlProviderTests

open FSharp.Data
open NUnit.Framework

type Config = FSharp.Data.YamlProvider<FilePath="/Users/chethusk/oss/FSharp.Data.YamlProvider/tests/FSharp.Data.YamlProvider.Tests/test.yml">

[<Test>]
let ``Can access properties of generative provider 2`` () =
    let obj = Config()
    Assert.AreEqual(obj.MyKey, "MyValue")
