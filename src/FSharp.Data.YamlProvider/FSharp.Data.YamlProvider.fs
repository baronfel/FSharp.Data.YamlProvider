module FSharp.Data.YamlProviderImplementation

open System
open System.Collections.Generic
open System.IO
open System.Reflection
open FSharp.Quotations
open FSharp.Core.CompilerServices
open ProviderImplementation.ProvidedTypes
open ProviderImplementation.ProvidedTypes.UncheckedQuotations
open YamlDotNet.Serialization
open YamlDotNet.Core

// Put any utility helpers here
[<AutoOpen>]
module internal Helpers =
    let dispose (x: IDisposable) = if x = null then () else x.Dispose()
    let debug msg = Printf.kprintf Diagnostics.Debug.WriteLine msg 
//    let debug msg = 
//        let dt = DateTime.Now
//        Printf.kprintf (fun msg -> System.IO.File.AppendAllText("/Users/chethusk/oss/FSharp.Data.YamlProvider/debug.log", sprintf "[%O] %s\n" dt msg)) msg

module File =
    let tryOpenFile filePath =
        try Some (new FileStream (filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
        with _ -> None

    let tryReadNonEmptyTextFile filePath =
        let maxAttempts = 5
        let rec sleepAndRun attempt = async {
            do! Async.Sleep 1000
            return! loop (attempt - 1) }

        and loop attemptsLeft = async {
            let attempt = maxAttempts - attemptsLeft + 1
            match tryOpenFile filePath with
            | Some file ->
                try
                    use reader = new StreamReader (file)
                    match attemptsLeft, reader.ReadToEnd() with
                    | 0, x -> return x
                    | _, "" ->
                        printfn "Attempt %d of %d: %s is empty. Sleep for 1 sec, then retry..." attempt maxAttempts filePath
                        return! sleepAndRun attemptsLeft
                    | _, content -> return content
                finally file.Dispose()
            | None ->
                if attemptsLeft = 0
                    then return raise (FileNotFoundException(sprintf "File, %s could not be opened after %d attempts." filePath maxAttempts))

                printfn "Attempt %d of %d: cannot read %s. Sleep for 1 sec, then retry..." attempt maxAttempts filePath
                return! sleepAndRun attemptsLeft }
        loop maxAttempts |> Async.RunSynchronously

    type private State =
        { LastFileWriteTime: DateTime
          Updated: DateTime }

    let watch changesOnly filePath onChanged =
        let getLastWrite() = File.GetLastWriteTime filePath
        let state = ref { LastFileWriteTime = getLastWrite(); Updated = DateTime.Now }

        let changed (_: FileSystemEventArgs) =
            let curr = getLastWrite()
            if curr <> (!state).LastFileWriteTime && DateTime.Now - (!state).Updated > TimeSpan.FromMilliseconds 500. then
              onChanged()
              state := { LastFileWriteTime = curr; Updated = DateTime.Now }

        let watcher = new FileSystemWatcher(Path.GetDirectoryName filePath, Path.GetFileName filePath)
        watcher.NotifyFilter <- NotifyFilters.CreationTime ||| NotifyFilters.LastWrite ||| NotifyFilters.Size
        watcher.Changed.Add changed
        if not changesOnly then
            watcher.Deleted.Add changed
            watcher.Renamed.Add changed
        watcher.EnableRaisingEvents <- true
        watcher :> IDisposable

    let getFullPath resolutionFolder fileName =
        if Path.IsPathRooted fileName then
          fileName
        else Path.Combine(resolutionFolder, fileName)

type FilePath = string

type private ContextMessage =
    | Watch of FilePath
    | AddDisposable of IDisposable
    | Cancel


type Context (provider: TypeProviderForNamespaces, cfg: TypeProviderConfig) =
    let watcher: IDisposable option ref = ref None

    let disposeWatcher() =
        !watcher |> Option.iter (fun x -> x.Dispose())
        watcher := None

    let watchForChanges (fileName: string) =
        disposeWatcher()
        let fileName = File.getFullPath cfg.ResolutionFolder fileName
        File.watch false fileName provider.Invalidate

    let agent = MailboxProcessor.Start(fun inbox ->
        let rec loop (files: Map<string, IDisposable>) (disposables: IDisposable list) = async {
            let unwatch file =
                match files |> Map.tryFind file with
                | Some disposable ->
                    disposable.Dispose()
                    files |> Map.remove file
                | None -> files

            let! msg = inbox.Receive()

            match msg with
            | Watch file -> return! loop (unwatch file |> Map.add file (watchForChanges file)) disposables
            | AddDisposable x -> return! loop files (x :: disposables)
            | Cancel ->
                files |> Map.toSeq |> Seq.map snd |> Seq.iter dispose
                disposables |> List.iter dispose
        }
        loop Map.empty []
    )

    member __.ResolutionFolder = cfg.ResolutionFolder
    member __.WatchFile (file: FilePath) = agent.Post (Watch file)
    member __.AddDisposable x = agent.Post (AddDisposable x)

    interface IDisposable with
        member __.Dispose() = agent.Post Cancel


[<Sealed>]
type MaybeBuilder () =
    // 'T -> M<'T>
    member inline __.Return value: 'T option =
        Some value

    // M<'T> -> M<'T>
    member inline __.ReturnFrom value: 'T option =
        value

    // unit -> M<'T>
    member inline __.Zero (): unit option =
        Some ()     // TODO: Should this be None?

    // (unit -> M<'T>) -> M<'T>
    member __.Delay (f: unit -> 'T option): 'T option =
        f ()

    // M<'T> -> M<'T> -> M<'T>
    // or
    // M<unit> -> M<'T> -> M<'T>
    member inline __.Combine (r1, r2: 'T option): 'T option =
        match r1 with
        | None ->
            None
        | Some () ->
            r2

    // M<'T> * ('T -> M<'U>) -> M<'U>
    member inline __.Bind (value, f: 'T -> 'U option): 'U option =
        Option.bind f value

    // 'T * ('T -> M<'U>) -> M<'U> when 'U :> IDisposable
    member __.Using (resource: ('T :> System.IDisposable), body: _ -> _ option): _ option =
        try body resource
        finally
            if not <| obj.ReferenceEquals (null, box resource) then
                resource.Dispose ()

    // (unit -> bool) * M<'T> -> M<'T>
    member x.While (guard, body: _ option): _ option =
        if guard () then
            // OPTIMIZE: This could be simplified so we don't need to make calls to Bind and While.
            x.Bind (body, (fun () -> x.While (guard, body)))
        else
            x.Zero ()

    // seq<'T> * ('T -> M<'U>) -> M<'U>
    // or
    // seq<'T> * ('T -> M<'U>) -> seq<M<'U>>
    member x.For (sequence: seq<_>, body: 'T -> unit option): _ option =
        // OPTIMIZE: This could be simplified so we don't need to make calls to Using, While, Delay.
        x.Using (sequence.GetEnumerator (), fun enum ->
            x.While (
                enum.MoveNext,
                x.Delay (fun () ->
                    body enum.Current)))

let maybe = MaybeBuilder()

[<RequireQualifiedAccess>]
module ValueParser =
    open System.Globalization

    /// Converts a function returning bool,value to a function returning value option.
    /// Useful to process TryXX style functions.
    let inline private tryParseWith func = func >> function
        | true, value -> Some value
        | false, _ -> None

    let (|Bool|_|) = tryParseWith Boolean.TryParse
    let (|Int|_|) = tryParseWith Int32.TryParse
    let (|Int64|_|) = tryParseWith Int64.TryParse
    let (|Float|_|) = tryParseWith (fun x -> Double.TryParse(x, NumberStyles.Any, CultureInfo.InvariantCulture))
    let (|TimeSpan|_|) = tryParseWith (fun x -> TimeSpan.TryParse(x, CultureInfo.InvariantCulture))
    let (|Guid|_|) = tryParseWith Guid.TryParse

    let (|DateTime|_|) =
        tryParseWith (fun x -> DateTime.TryParse(x, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal))

    let (|Uri|_|) (text: string) =
        ["http"; "https"; "ftp"; "ftps"; "sftp"; "amqp"; "file"; "ssh"; "tcp"]
        |> List.tryPick (fun x ->
            if text.Trim().StartsWith(x + ":", StringComparison.InvariantCultureIgnoreCase) then
                match System.Uri.TryCreate(text, UriKind.Absolute) with
                | true, uri -> Some uri
                | _ -> None
            else None)

type Helper () = 
    static member CreateResizeArray<'a>(data: 'a seq) = ResizeArray<'a>(data)

module private Parser =
    type Scalar =
        | Int of int
        | Int64 of int64
        | String of string
        | TimeSpan of TimeSpan
        | Bool of bool
        | Uri of Uri
        | Float of double
        | Guid of Guid

        static member ParseStr = function
            | ValueParser.Bool x -> Bool x
            | ValueParser.Int x -> Int x
            | ValueParser.Int64 x -> Int64 x
            | ValueParser.Float x -> Float x
            | ValueParser.TimeSpan x -> TimeSpan x
            | ValueParser.Uri x -> Uri x
            | ValueParser.Guid x -> Guid x
            | x -> String x

        static member FromObj (inferTypesFromStrings: bool) : obj -> Scalar = function
            | null -> String ""
            | :? System.Boolean as b -> Bool b
            | :? System.Int32 as i -> Int i
            | :? System.Int64 as i -> Int64 i
            | :? System.Double as d -> Float d
            | :? System.String as s ->
                if inferTypesFromStrings then Scalar.ParseStr s
                else Scalar.String s
            | t -> failwithf "Unknown type %s" (string (t.GetType()))

        member x.UnderlyingType =
            match x with
            | Int x -> x.GetType()
            | Int64 x -> x.GetType()
            | String x -> x.GetType()
            | Bool x -> x.GetType()
            | TimeSpan x -> x.GetType()
            | Uri x -> x.GetType()
            | Float x -> x.GetType()
            | Guid x -> x.GetType()

        member x.BoxedValue =
            match x with
            | Int x -> box x
            | Int64 x -> box x
            | String x -> box x
            | TimeSpan x -> box x
            | Bool x -> box x
            | Uri x -> box x
            | Float x -> box x
            | Guid x -> box x

    type Node =
        | Scalar of Scalar
        | List of Node list
        | Map of (string * Node) list

    let parse (inferTypesFromStrings: bool) : (string -> Node) =
        let rec loop (n: obj) =
            match n with
            | :? List<obj> as l -> Node.List (l |> Seq.map loop |> Seq.toList)
            | :? Dictionary<obj,obj> as map ->
                map
                |> Seq.map (fun p -> string p.Key, loop p.Value)
                |> Seq.toList
                |> Map
            | scalar -> Scalar (Scalar.FromObj inferTypesFromStrings scalar)

        //let settings = SerializerSettings (EmitDefaultValues = true, EmitTags = false, SortKeyForMapping = false)
        let deserializer = DeserializerBuilder().Build()
        fun text ->
            try
                deserializer.Deserialize(text) |> loop
            with
              | :? YamlException as e when e.InnerException <> null ->
                  raise e.InnerException // inner exceptions are much more informative
              | _ -> reraise()

    let private inferListType (targetType: Type) (nodes: Node list) =
        let types =
            nodes
            |> List.choose (function Scalar x -> Some x | _ -> None)
            |> Seq.groupBy (fun n -> n.UnderlyingType)
            |> Seq.map fst
            |> Seq.toList

        match types with
        | [] -> targetType
        | [ty] -> typedefof<ResizeArray<_>>.MakeGenericType ty
        | types -> failwithf "List cannot contain elements of heterohenius types (attempt to mix types: %A)." types

    let update (target: 'a) (updater: Node) =
        let tryGetField x name = x.GetType().GetField(name, BindingFlags.Instance ||| BindingFlags.NonPublic) |> Option.ofObj

        let getChangedDelegate x =
            x.GetType().GetField("_changed", BindingFlags.Instance ||| BindingFlags.NonPublic).GetValue x :?> MulticastDelegate

        let rec update (target: obj) name (updater: Node) =
            match name, updater with
            | _, Scalar x -> updateScalar target name x
            | _, Map m -> updateMap target name m
            | Some name, List l -> updateList target name l
            | None, _ -> failwithf "Only Maps are allowed at the root level."

        and updateScalar (target: obj) name (node: Scalar) =
            maybe {
                let! name = name
                let! field = tryGetField target ("_" + name)

                let newValue =
                    if field.FieldType <> node.UnderlyingType then
                        if node.UnderlyingType <> typeof<string> && field.FieldType = typeof<string> then
                            node.BoxedValue |> string |> box
                        elif node.UnderlyingType = typeof<int32> && field.FieldType = typeof<int64> then
                            node.BoxedValue :?> int32 |> int64 |> box
                        else
                            failwithf "Cannot assign value of type %s to field of %s: %s." node.UnderlyingType.Name name field.FieldType.Name
                    else
                        node.BoxedValue

                let oldValue = field.GetValue(target)

                return!
                    if oldValue <> newValue then
                        field.SetValue(target, newValue)
                        Some (getChangedDelegate target)
                    else None
            } |> Option.toList

        and makeListItemUpdaters (itemType: Type) (itemNodes: Node list) =
            itemNodes
            |> List.choose (function
                | Scalar x -> Some x.BoxedValue
                | Map m ->
                    let mapItem = Activator.CreateInstance itemType
                    updateMap mapItem None m |> ignore
                    Some mapItem
                | List l -> Some (fillList itemType l))

        and makeListInstance (listType: Type) (itemType: Type) (updaters: obj list) =
            let list = Activator.CreateInstance listType
            let addMethod = listType.GetMethod("Add", [|itemType|])
            for updater in updaters do
                addMethod.Invoke(list, [|updater|]) |> ignore
            list

        and fillList (targetType: Type) (updaters: Node list) =
            let fieldType = inferListType targetType updaters
            let itemType = fieldType.GetGenericArguments().[0]
            let updaters = makeListItemUpdaters itemType updaters

            if not (targetType.IsAssignableFrom fieldType) then
                failwithf "Cannot assign %O to %O." fieldType.Name targetType.Name

            makeListInstance fieldType itemType updaters

        and updateList (target: obj) name (updaters: Node list) =
            maybe {
                let! field = tryGetField target ("_" + name)
                let fieldType = inferListType field.FieldType updaters

                if field.FieldType <> fieldType then
                    failwithf "Cannot assign %O to %O." fieldType.Name field.FieldType.Name

                let isComparable (x: obj) = x :? Uri || x :? IComparable
                let values = field.GetValue target :?> Collections.IEnumerable |> Seq.cast<obj>
                // NOTE: another solution would be to make our provided type implement IComparable
                // On the other side I'm not completely sure why we sort at all.
                // What if the ordering of the item matters for the user?
                let isSortable = values |> Seq.forall isComparable

                let sort (xs: obj seq) =
                    xs
                    |> Seq.sortBy (function
                       | :? Uri as uri -> uri.OriginalString :> IComparable
                       | :? IComparable as x -> x
                       | x -> failwithf "%A is not comparable, so it cannot be included into a list."  x)
                    |> Seq.toList

                let itemType = fieldType.GetGenericArguments().[0]
                let updaters = makeListItemUpdaters itemType updaters

                let oldValues, newValues =
                    if isSortable then sort values, sort updaters
                    else Seq.toList values, updaters

                return!
                    if not isSortable || oldValues <> newValues then
                        let list = makeListInstance fieldType itemType updaters
                        field.SetValue(target, list)
                        Some (getChangedDelegate target)
                    else None
            } |> function Some dlg -> [dlg] | None -> []

        and updateMap (target: obj) name (updaters: (string * Node) list) =
            let target =
                maybe {
                    let! name = name
                    let ty = target.GetType()
                    let mapProp = Option.ofObj (ty.GetProperty name)
                    return!
                        match mapProp with
                        | None ->
                            debug "Type %s does not contain %s property." ty.Name name
                            None
                        | Some prop -> Some (prop.GetValue (target, [||]))
                } |> Option.defaultValue target

            match updaters |> List.collect (fun (name, node) -> update target (Some name) node) with
            | [] -> []
            | events -> getChangedDelegate target :: events // if any child is raising the event, we also do (pull it up the hierarchy)

        update target None updater
        |> Seq.filter ((<>) null)
        |> Seq.collect (fun x -> x.GetInvocationList())
        |> Seq.distinct
        //|> fun x -> printfn "Updated. %d events to raise: %A" (Seq.length x) x; Seq.toList x
        |> Seq.iter (fun h -> h.Method.Invoke(h.Target, [|box target; EventArgs.Empty|]) |> ignore)

module private TypesFactory =
    open Parser

    type Scalar with
        member x.ToExpr() =
            match x with
            | Int x -> Expr.Value x
            | Int64 x -> Expr.Value x
            | String x -> Expr.Value x
            | Bool x -> Expr.Value x
            | Float x -> Expr.Value x
            | TimeSpan x ->
                let parse = typeof<TimeSpan>.GetMethod("Parse", [|typeof<string>|])
                Expr.Call(parse, [Expr.Value (x.ToString())])
            | Uri x ->
                let ctr = typeof<Uri>.GetConstructor [|typeof<string>|]
                Expr.NewObject(ctr, [Expr.Value x.OriginalString])
            | Guid x ->
                let parse = typeof<Guid>.GetMethod("Parse", [|typeof<string>|])
                Expr.Call(parse, [Expr.Value (x.ToString())])

    type T =
        { MainType: Type option
          Types: MemberInfo list
          Init: Expr -> Expr }

    let private generateChangedEvent =
        let eventType = typeof<EventHandler>
        let delegateType = typeof<Delegate>
        let combineMethod = delegateType.GetMethod("Combine", [|delegateType; delegateType|])
        let removeMethod = delegateType.GetMethod("Remove", [|delegateType; delegateType|])

        fun() ->
            let eventField = ProvidedField("_changed", eventType)

            let changeEvent m me v =
                let current = Expr.Coerce (Expr.FieldGet(me, eventField), delegateType)
                let other = Expr.Coerce (v, delegateType)
                Expr.Coerce (Expr.Call (m, [current; other]), eventType)

            let adder = changeEvent combineMethod
            let remover = changeEvent removeMethod

            let event =
                ProvidedEvent(
                    "Changed",
                    eventType,
                    adderCode = (fun [me; v] -> Expr.FieldSet(me, eventField, adder me v)),
                    removerCode = (fun [me; v] -> Expr.FieldSet(me, eventField, remover me v))
                )
            eventField, event

    let rec transform readOnly name (node: Node) =
        match name, node with
        | Some name, Scalar x -> transformScalar readOnly name x
        | _, Map m -> transformMap readOnly name m
        | Some name, List l -> transformList readOnly name true l
        | None, _ -> failwithf "Only Maps are allowed at the root level."

    and transformScalar readOnly name (node: Scalar) =
        let rawType = node.UnderlyingType
        let field = ProvidedField("_" +  name, rawType)
        let prop =
            if readOnly
            then ProvidedProperty (name, rawType, isStatic = false, getterCode = (fun [me] -> Expr.FieldGet(me, field)))
            else ProvidedProperty (name, rawType, isStatic = false, getterCode = (fun [me] -> Expr.FieldGet(me, field)), setterCode = (fun [me;v] -> Expr.FieldSet(me, field, v)))
        let initValue = node.ToExpr()

        { MainType = Some rawType
          Types = [field :> MemberInfo; prop :> MemberInfo]
          Init = fun me -> Expr.FieldSet(me, field, initValue) }

    and transformList readOnly name generateField (children: Node list) =
        let elements =
            children
            |> List.map (function
               | Scalar x ->
                   { MainType = Some x.UnderlyingType
                     Types = []
                     Init = fun _ -> x.ToExpr() }
               | Map m -> transformMap readOnly None m
               | List l -> transformList readOnly (name + "_Items") false l)

        let elements, elementType =
            match elements |> Seq.groupBy (fun n -> n.MainType) |> Seq.map fst |> Seq.toList with
            | [Some ty] -> elements, ty
            | [None] ->
                // Sequence of maps: https://github.com/fsprojects/FSharp.Configuration/issues/51
                // TODOL Construct the type from all the elements (instead of only the first entry)
                let headChildren = match children |> Seq.head with Map m -> m | _ -> failwith "expected a sequence of maps."

                let childTypes, childInits = foldChildren readOnly headChildren
                let eventField, event = generateChangedEvent()

                let mapTy =
                    ProvidedTypeDefinition(
                        name + "_Item_Type",
                        Some typeof<obj>,
                        hideObjectMethods = true,
                        isErased = false,
                        SuppressRelocation = false
                    )

                let ctr = ProvidedConstructor([], invokeCode = (fun [me] -> childInits me))
                mapTy.AddMembers (ctr :> MemberInfo :: childTypes)
                mapTy.AddMember eventField
                mapTy.AddMember event
                [{ MainType = Some (mapTy :> _)
                   Types = [mapTy :> MemberInfo]
                   Init = fun _ -> Expr.NewObject(ctr, []) }],
                mapTy :> _
            | types -> failwithf "List cannot contain elements of heterogeneous types (attempt to mix types: %A)."
                                 (types |> List.map (Option.map (fun x -> x.Name)))

        let propType = ProvidedTypeBuilder.MakeGenericType(typedefof<IList<_>>, [elementType])
        let ctrType = ProvidedTypeBuilder.MakeGenericType(typedefof<seq<_>>, [elementType])

        let listCtr =
            let meth = typeof<Helper>.GetMethod("CreateResizeArray")
            ProvidedTypeBuilder.MakeGenericMethod(meth, [elementType])

        let childTypes = elements |> List.collect (fun x -> x.Types)

        let initValue ty me =
            Expr.Coerce(
                Expr.CallUnchecked(listCtr, [Expr.Coerce(Expr.NewArray(elementType, elements |> List.map (fun x -> x.Init me)), ctrType)]),
                ty)

        if generateField then
            let fieldType = ProvidedTypeBuilder.MakeGenericType(typedefof<ResizeArray<_>>, [elementType])
            let field = ProvidedField("_" + name, fieldType)

            let prop =
                if readOnly
                then ProvidedProperty (name, propType, isStatic=false,
                        getterCode = (fun [me] -> Expr.Coerce(Expr.FieldGet(me, field), propType)))
                else ProvidedProperty (name, propType, isStatic=false,
                        getterCode = (fun [me] -> Expr.Coerce(Expr.FieldGet(me, field), propType)),
                        setterCode = (fun [me; v] -> Expr.FieldSet(me, field, Expr.Coerce(Expr.CallUnchecked(listCtr, [Expr.Coerce(v, ctrType)]), fieldType))))

            { MainType = Some fieldType
              Types = childTypes @ [field :> MemberInfo; prop :> MemberInfo]
              Init = fun me -> Expr.FieldSet(me, field, initValue fieldType me) }
        else
            { MainType = Some propType
              Types = childTypes
              Init = initValue propType }

    and foldChildren readOnly (children: (string * Node) list) =
        let childTypes, childInits =
            children
            |> List.map (fun (name, node) -> transform readOnly (Some name) node)
            |> List.fold (fun (types, inits) t -> types @ t.Types, inits @ [t.Init]) ([], [])

        let affinedChildInits me =
            childInits
            |> List.fold (fun acc expr -> expr me :: acc) []
            |> List.reduce (fun res expr -> Expr.Sequential(res, expr))

        childTypes, affinedChildInits

    and transformMap readOnly name (children: (string * Node) list) =
        let childTypes, childInits = foldChildren readOnly children
        let eventField, event = generateChangedEvent()
        match name with
        | Some name ->
            let mapTy = ProvidedTypeDefinition(name + "_Type", Some typeof<obj>, hideObjectMethods = true,
                                               isErased = false, SuppressRelocation = false)
            let ctr = ProvidedConstructor([], invokeCode = (fun [me] -> childInits me))
            mapTy.AddMembers (ctr :> MemberInfo :: childTypes)
            let field = ProvidedField("_" + name, mapTy)
            let prop = ProvidedProperty (name, mapTy, isStatic = false, getterCode = (fun [me] -> Expr.FieldGet(me, field)))
            mapTy.AddMember eventField
            mapTy.AddMember event

            { MainType = Some (mapTy :> _)
              Types = [mapTy :> MemberInfo; field :> MemberInfo; prop :> MemberInfo]
              Init = fun me -> Expr.FieldSet(me, field, Expr.NewObject(ctr, [])) }
        | None ->
            { MainType = None
              Types = [eventField :> MemberInfo; event :> MemberInfo] @ childTypes
              Init = childInits }

type Root (inferTypesFromStrings: bool) =
    let serializer =
        SerializerBuilder()
            .WithTypeConverter(
            {   new IYamlTypeConverter with
                    member __.Accepts ty =
                        ty = typeof<TimeSpan>
                    member __.ReadYaml(parser, ty) =
                        failwith "Not implemented"
                    member __.WriteYaml(emitter:YamlDotNet.Core.IEmitter, value, ty) =
                        match value with
                        | :? TimeSpan as ts ->
                            let formattedValue = ts.ToString("G")
                            emitter.Emit(YamlDotNet.Core.Events.Scalar(null, formattedValue));
                        | _ -> failwithf "Expected TimeSpan but received %A" value
            })
            .Build()

    let mutable lastLoadedFrom = None

    let errorEvent = new Event<Exception>()

    /// Load Yaml config as text and update itself with it.
    member x.LoadText (yamlText: string) =
      try
        Parser.parse inferTypesFromStrings yamlText |> Parser.update x
      with e ->
        async { errorEvent.Trigger e } |> Async.Start
        reraise()

    /// Load Yaml config from a TextReader and update itself with it.
    member x.Load (reader: TextReader) =
      try
        reader.ReadToEnd() |> Parser.parse inferTypesFromStrings |> Parser.update x
      with e ->
        async { errorEvent.Trigger e } |> Async.Start
        reraise()

    /// Load Yaml config from a file and update itself with it.
    member x.Load (filePath: string) =
      try
        filePath |> File.tryReadNonEmptyTextFile |> x.LoadText
        lastLoadedFrom <- Some filePath
      with e ->
        async { errorEvent.Trigger e } |> Async.Start
        reraise()

    /// Load Yaml config from a file, update itself with it, then start watching it for changes.
    /// If it detects any change, it reloads the file.
    member x.LoadAndWatch (filePath: string) =
        x.Load filePath
        File.watch true filePath <| fun _ ->
            Diagnostics.Debug.WriteLine (sprintf "Loading %s..." filePath)
            try
                x.Load filePath
            with e ->
                Diagnostics.Debug.WriteLine (sprintf "Cannot load file %s: %O" filePath e.Message)

    /// Saves configuration as Yaml text into a stream.
    member x.Save (stream: Stream) =
        use writer = new StreamWriter(stream)
        x.Save writer

    /// Saves configuration as Yaml text into a TextWriter.
    member x.Save (writer: TextWriter) = serializer.Serialize(writer, x)

    /// Saves configuration as Yaml text into a file.
    member x.Save (filePath: string) =
        // forbid any access to the file for atomicity
        use file = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None)
        x.Save file

    /// Saves configuration as Yaml text into the last file it was loaded (if any).
    /// Throws InvalidOperationException if configuration has not been loaded at all or if it has loaded from
    /// a different kind of source (string or TextReader).
    member x.Save () =
        match lastLoadedFrom with
        | Some filePath -> x.Save filePath
        | None -> invalidOp "Cannot save configuration because it was not loaded from a file."

    /// Returns content as Yaml text.
    override x.ToString() =
        use writer = new StringWriter()
        x.Save writer
        writer.ToString()

    // Error channel to announce parse errors on
    [<CLIEvent>]
    member __.Error = errorEvent.Publish

let internal typedYamlConfig (context: Context) =
    let baseTy = typeof<Root>
    
    debug "creating TP"
    try
        let asm = Assembly.GetExecutingAssembly()
        let yamlConfig = ProvidedTypeDefinition(asm, "FSharp.Data", "YamlProvider", Some baseTy, hideObjectMethods = true, isSealed = true, isErased = false)
        debug "made config"
        let staticParams =
            [ ProvidedStaticParameter ("FilePath", typeof<string>, "")
              ProvidedStaticParameter ("ReadOnly", typeof<bool>, false)
              ProvidedStaticParameter ("YamlText", typeof<string>, "")
              ProvidedStaticParameter ("InferTypesFromStrings", typeof<bool>, true) ]
    
        yamlConfig.AddXmlDoc
            """<summary>Statically typed YAML config.</summary>
               <param name='FilePath'>Path to YAML file.</param>
               <param name='ReadOnly'>Whether the resulting properties will be read-only or not.</param>
               <param name='YamlText'>Yaml as text. Mutually exclusive with FilePath parameter.</param>"""
    
        yamlConfig.DefineStaticParameters(
            parameters = staticParams,
            instantiationFunction = fun typeName paramValues ->
                let createTy yaml readOnly inferTypesFromStrings =
                    debug "creating type"
                    let myAssem = ProvidedAssembly()
                    let ty = ProvidedTypeDefinition (myAssem, "FSharp.Data", typeName, Some baseTy, isErased = false, hideObjectMethods = true)
                    let types = TypesFactory.transform readOnly None (Parser.parse inferTypesFromStrings yaml)
                    let ctr = ProvidedConstructor([], invokeCode = fun (me :: _) -> debug "parameterless constructore called"; types.Init me)
                    let baseCtor = baseTy.GetConstructor(BindingFlags.Public ||| BindingFlags.Instance, null, [|typeof<bool>|], null)
                    ctr.BaseConstructorCall <- fun [me] -> baseCtor, [me; Expr.Value inferTypesFromStrings]
                    ty.AddMembers (ctr :> MemberInfo :: types.Types)
                    myAssem.AddTypes [ty]
                    ty
                debug "got param values %A" paramValues
                match paramValues with
                | [| :? string as filePath; :? bool as readOnly; :? string as yamlText; :? bool as inferTypesFromStrings |] ->
                     match filePath, yamlText with
                     | "", "" -> failwith "You must specify either FilePath or YamlText parameter."
                     | "", yamlText -> createTy yamlText readOnly inferTypesFromStrings
                     | filePath, _ ->
                          let filePath =
                              if Path.IsPathRooted filePath
                              then filePath
                              else Path.Combine(context.ResolutionFolder, filePath)
                              |> Path.GetFullPath
                          context.WatchFile filePath
                          createTy (File.ReadAllText filePath) readOnly inferTypesFromStrings
                | _ -> failwith "Wrong parameters")
        debug "defined static params"
        
        yamlConfig
      with
      | ex -> 
        debug "Error: %s\n%s" ex.Message ex.StackTrace
        reraise()

[<TypeProvider>]
type YamlProvider (config : TypeProviderConfig) as this =
    inherit TypeProviderForNamespaces (config, addDefaultProbingLocation = true, assemblyReplacementMap = [("FSharp.Data.YamlProviderImplementation", "YamlProvider")])

    let ctx = new Context(this, config)
    do this.RegisterRuntimeAssemblyLocationAsProbingFolder config
    do this.AddNamespace("FSharp.Data", [ typedYamlConfig ctx ])
    do this.Disposing.Add (fun _ -> dispose ctx)

[<TypeProviderAssembly>]
do ()
