{
     Copyright 2009-2011 10gen Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
}
{ This unit implements BSON, a binary JSON-like document format.
  It is used to represent documents in MongoDB and also for network traffic.
  See http://www.mongodb.org/display/DOCS/BSON }

unit MongoBson;

interface

uses
  Windows, DB;

type
  TBson = class;
  TIntegerArray = array of Integer;
  TDoubleArray = array of Double;
  TBooleanArray = array of Boolean;
  TStringArray = array of string;

  { A value of TBsonType indicates the type of the data associated
    with a field within a BSON document. }
  TBsonType = (
    bsonEOO = 0,
    bsonDOUBLE = 1,
    bsonSTRING = 2,
    bsonOBJECT = 3,
    bsonARRAY = 4,
    bsonBINDATA = 5,
    bsonUNDEFINED = 6,
    bsonOID = 7,
    bsonBOOL = 8,
    bsonDATE = 9,
    bsonNULL = 10,
    bsonREGEX = 11,
    bsonDBREF = 12, (* Deprecated. *)
    bsonCODE = 13,
    bsonSYMBOL = 14,
    bsonCODEWSCOPE = 15,
    bsonINT = 16,
    bsonTIMESTAMP = 17,
    bsonLONG = 18);

  TBsonIterator = class;

  { A TBsonOID is used to store BSON Object IDs.
    See http://www.mongodb.org/display/DOCS/Object+IDs }
  TBsonOID = class(TObject)
    var
      { the oid data }
      value : array[0..11] of Byte;
    { Generate an Object ID }
    constructor Create(); overload;
    { Create an ObjectID from a 24-digit hex string }
    constructor Create(s : string); overload;
    { Create an Object ID from a TBsonIterator pointing to an oid field }
    constructor Create(i : TBsonIterator); overload;
    { Convert this Object ID to a 24-digit hex string }
    function AsString() : string;
  end;

  { A TBsonCodeWScope is used to hold javascript code and its associated scope.
    See TBsonIterator.getCodeWScope() }
  TBsonCodeWScope = class(TObject)
    var
      code : string;
      scope : TBson;
    { Create a TBsonCodeWScope from a javascript string and a TBson scope }
    constructor Create(code_ : string; scope_ : TBson); overload;
    { Create a TBsonCodeWScope from a TBSonIterator pointing to a
      CODEWSCOPE field. }
    constructor Create(i : TBsonIterator); overload;
  end;

  { A TBsonRegex is used to hold a regular expression string and its options.
    See TBsonIterator.getRegex(). }
  TBsonRegex = class(TObject)
    var
      pattern : string;
      options : string;
    { Create a TBsonRegex from reqular expression and options strings }
    constructor Create(pattern_ : string; options_ : string); overload;
    { Create a TBsonRegex from a TBsonIterator pointing to a REGEX field }
    constructor Create(i : TBsonIterator); overload;
  end;

  { A TBsonTimestamp is used to hold a TDateTime and an increment value.
    See http://www.mongodb.org/display/DOCS/Timestamp+data+type and
    TBsonIterator.getTimestamp() }
  TBsonTimestamp = class(TObject)
    var
      time : TDateTime;
      increment : Integer;
    { Create a TBsonTimestamp from a TDateTime and an increment }
    constructor Create(time_ : TDateTime; increment_ : Integer); overload;
    { Create a TBSonTimestamp from a TBsonIterator pointing to a TIMESTAMP
      field. }
    constructor Create(i : TBsonIterator); overload;
  end;

  { A TBsonBinary is used to hold the contents of BINDATA fields.
    See TBsonIterator.getBinary() }
  TBsonBinary = class(TObject)
    var
      { Pointer to data allocated on the heap with GetMem }
      data : Pointer;
      { The length of the data in bytes }
      len  : Integer;
      { The subtype of the BINDATA (usually 0) }
      kind : Integer;
    { Create a TBsonBinary from a pointer and a length.  The data
      is copied to the heap.  kind is initialized to 0 }
    constructor Create(p : Pointer; length : Integer); overload;
    { Create a TBsonBinary from a TBsonIterator pointing to a BINDATA
      field. }
    constructor Create(i : TBsonIterator); overload;
    { Destroys the TBsonBinary and releases its memory with FreeMem() }
    destructor Destroy(); override;
  end;

  { A TBsonBuffer is used to build a BSON document by appending the
    names and values of fields.  Call finish() when done to convert
    the buffer to a TBson which can be used in database operations.
    Example: @longcode(#
      var
        bb : TBsonBuffer;
        b  : TBson;
      begin
        bb := TBsonBuffer.Create();
        bb.append('name', 'Joe');
        bb.append('age', 33);
        bb.append('city', 'Boston');
        b := bb.finish();
      end;
    #) }
  TBsonBuffer = class(TObject)
    private
      var handle : Pointer;
    public
      { Create an empty TBsonBuffer ready to have fields appended. }
      constructor Create();
      { Append a string (PAnsiChar) to the buffer }
      function append(name : string; value : PAnsiChar) : Boolean; overload;
      { Append an Integer to the buffer }
      function append(name : string; value : Integer) : Boolean; overload;
      { Append an Int64 to the buffer }
      function append(name : string; value : Int64) : Boolean; overload;
      { Append a Double to the buffer }
      function append(name : string; value : Double) : Boolean; overload;
      { Append a TDateTime to the buffer; converted to 64-bit POSIX time }
      function append(name : string; value : TDateTime) : Boolean; overload;
      { Append a Boolean to the buffer }
      function append(name : string; value : Boolean) : Boolean; overload;
      { Append an Object ID to the buffer }
      function append(name : string; value : TBsonOID) : Boolean; overload;
      { Append a CODEWSCOPE to the buffer }
      function append(name : string; value : TBsonCodeWScope) : Boolean; overload;
      { Append a REGEX to the buffer }
      function append(name : string; value : TBsonRegex) : Boolean; overload;
      { Append a TIMESTAMP to the buffer }
      function append(name : string; value : TBsonTimestamp) : Boolean; overload;
      { Append BINDATA to the buffer }
      function append(name : string; value : TBsonBinary) : Boolean; overload;
      { Append a TBson document as a subobject }
      function append(name : string; value : TBson) : Boolean; overload;
      { Generic version of append.  Calls one of the other append functions
        if the type contained in the variant is supported. }
      function append(name : string; value : OleVariant) : Boolean; overload;
      { Append an array of Integers }
      function appendArray(name : string; value : array of Integer) : Boolean; overload;
      { Append an array of Doubles }
      function appendArray(name : string; value : array of Double) : Boolean; overload;
      { Append an array of Booleans }
      function appendArray(name : string; value : array of Boolean) : Boolean; overload;
      { Append an array of strings }
      function appendArray(name : string; value : array of string) : Boolean; overload;
      { Append a NULL field to the buffer }
      function appendNull(name : string) : Boolean;
      { Append an UNDEFINED field to the buffer }
      function appendUndefined(name : string) : Boolean;
      { Append javascript code to the buffer }
      function appendCode(name : string; value : PAnsiChar) : Boolean;
      { Append a SYMBOL to the buffer }
      function appendSymbol(name : string; value : PAnsiChar) : Boolean;
      { Alternate way to append BINDATA directly without first creating a
        TBsonBinary value }
      function appendBinary(name : string; kind : Integer; data : Pointer; length : Integer) : Boolean;
      { Indicate that you will be appending more fields as a subobject }
      function startObject(name : string) : Boolean;
      { Indicate that you will be appending more fields as an array }
      function startArray(name : string) : Boolean;
      { Indicate that a subobject or array is done. }
      function finishObject() : Boolean;
      { Return the current size of the BSON document you are building }
      function size() : Integer;
      { Call this when finished appending fields to the buffer to turn it into
        a TBson for network transport. }
      function finish() : TBson;
      { Destroy this TBsonBuffer.  Releases external resources. }
      destructor Destroy(); override;
  end;

  { A TBson holds a BSON document.  BSON is a binary, JSON-like document format.
    It is used to represent documents in MongoDB and also for network traffic.
    See http://www.mongodb.org/display/DOCS/BSON   }
  TBson = class(TObject)
  private
    fDS: TDataSet;

    function GetFieldDefs: TFieldDefs;
    procedure ConvertToString(var AString: string; i: TBsonIterator; depth: integer);
  public
      { Pointer to externally managed data.  User code should not modify this.
        It is public only because the MongoDB and GridFS units must access it. }
      var handle : Pointer;
      { Return the size of this BSON document in bytes }
      function size() : Integer;
      { Get a TBsonIterator that points to the first field of this BSON }
      function iterator() : TBsonIterator;
      { Get a TBsonIterator that points to the field with the given name.
        If name is not found, nil is returned. }
      function find(name : string) : TBsonIterator;
      { Get the value of a field given its name.  This function does not support
        all BSON field types.  Use find() and one of the 'get' functions of
        TBsonIterator to retrieve special values. }
      function value(name : string) : Variant;
      { Display this BSON document on the console.  subobjects and arrays are
        appropriately indented. }
      procedure display();
      { Create a TBson given a pointer to externally managed data describing
        the document.  User code should not instantiate TBson directly.  Use
        TBsonBuffer and finish() to create BSON documents. }
      constructor Create(h : Pointer);
      { Destroy this TBson.  Releases the externally managed data. }
      destructor Destroy; override;
      function AsString: string;

      property FieldDefs: TFieldDefs read GetFieldDefs;
  end;

  { TBsonIterators are used to step through the fields of a TBson document. }
  TBsonIterator = class(TObject)
    private
       { Pointer to externally managed data. }
       var handle : Pointer;
    public
      { Return the TBsonType of the field pointed to by this iterator. }
      function kind() : TBsonType;
      { Return the key (or name) of the field pointed to by this iterator. }
      function key() : string;
      { Step to the first or next field of a TBson document.  Returns True
        if there is a next field; otherwise, returns false at the end of the
        document (or subobject).
        Example: @longcode(#
          iter := b.iterator;
          while i.next() do
             if i.kind = bsonNULL then
                WriteLn(i.key, ' is a NULL field.');
        #) }
      function next() : Boolean;
      { Get the value of the field pointed to by this iterator.  This function
        does not support all BSON field types and will throw an exception for
        those it does not.  Use one of the 'get' functions to extract one of these
        special types. }
      function value() : Variant;
      { Get an TBsonIterator pointing to the first field of a subobject or array.
        kind() must be bsonOBJECT or bsonARRAY. }
      function subiterator() : TBsonIterator;
      { Get an Object ID from the field pointed to by this iterator. }
      function getOID() : TBsonOID;
      { Get a TBsonCodeWScope object for a CODEWSCOPE field pointed to by this
        iterator. }
      function getCodeWScope() : TBsonCodeWScope;
      { Get a TBsonRegex for a REGEX field }
      function getRegex() : TBsonRegex;
      { Get a TBsonTimestamp object for a TIMESTAMP field pointed to by this
        iterator. }
      function getTimestamp() : TBsonTimestamp;
      { Get a TBsonBinary object for the BINDATA field pointed to by this
        iterator. }
      function getBinary() : TBsonBinary;
      { Get an array of Integers.  This iterator must point to ARRAY field
        which has each component type as Integer }
      function getIntegerArray() : TIntegerArray;
      { Get an array of Doubles.  This iterator must point to ARRAY field
        which has each component type as Double }
      function getDoubleArray() : TDoubleArray;
      { Get an array of strings.  This iterator must point to ARRAY field
        which has each component type as string }
      function getStringArray() : TStringArray;
      { Get an array of Booleans.  This iterator must point to ARRAY field
        which has each component type as Boolean }
      function getBooleanArray() : TBooleanArray;
      { Internal usage only.  Create an uninitialized TBsonIterator }
      constructor Create(); overload;
      { Create a TBsonIterator that points to the first field of the given
        TBson }
      constructor Create(b : TBson); overload;
      { Destroy this TBsonIterator.  Releases external resources. }
      destructor Destroy; override;
    end;

    var
    { An empty BSON document }
      bsonEmpty : TBson;

    (* The idea for this shorthand way to build a BSON
       document from an array of variants came from Stijn Sanders
       and his TMongoWire, located here:
       https://github.com/stijnsanders/TMongoWire

       Subobjects are started with '{' and ended with '}'

       Example: @longcode(#
         var b : TBson;
         begin
           b := BSON(['name', 'Albert', 'age', 64,
                       'address', '{',
                          'street', '109 Vine Street',
                          'city', 'New Haven',
                          '}' ]);
    #) *)
    function BSON(x : array of OleVariant) : TBson;

    { Convert a byte to a 2-digit hex string }
    function ByteToHex(InByte : Byte) : string;

{ Convert an Int64 to a Double.  Some loss of precision may occur. }
type
  TInt64toDouble = function(i64 : int64) : double; cdecl;
var
  Int64toDouble: TInt64toDouble;

implementation
  uses SysUtils, Variants;

type
  Tset_bson_err_handler = procedure(err_handler : Pointer); cdecl;
  Tbson_create = function: Pointer; cdecl;
  Tbson_init = procedure(b : Pointer);  cdecl;
  Tbson_destroy = procedure(b : Pointer); cdecl;
  Tbson_dispose = procedure(b : Pointer); cdecl;
  Tbson_copy = procedure(dest : Pointer; src : Pointer); cdecl;
  Tbson_finish = function(b : Pointer) : Integer; cdecl;
  Tbson_oid_gen = procedure(oid : Pointer); cdecl;
  Tbson_oid_to_string = procedure(oid : Pointer; s : PAnsiChar); cdecl;
  Tbson_oid_from_string = procedure(oid : Pointer; s : PAnsiChar); cdecl;
  Tbson_append_string = function(b : Pointer; name : PAnsiChar; value : PAnsiChar) : Integer; cdecl;
  Tbson_append_code = function(b : Pointer; name : PAnsiChar; value : PAnsiChar) : Integer; cdecl;
  Tbson_append_symbol = function(b : Pointer; name : PAnsiChar; value : PAnsiChar) : Integer; cdecl;
  Tbson_append_int = function(b : Pointer; name : PAnsiChar; value : Integer) : Integer; cdecl;
  Tbson_append_long = function(b : Pointer; name : PAnsiChar; value : Int64) : Integer; cdecl;
  Tbson_append_double = function(b : Pointer; name : PAnsiChar; value : Double) : Integer; cdecl;
  Tbson_append_date = function(b : Pointer; name : PAnsiChar; value : Int64) : Integer; cdecl;
  Tbson_append_bool = function(b : Pointer; name : PAnsiChar; value : Boolean) : Integer; cdecl;
  Tbson_append_null = function(b : Pointer; name : PAnsiChar) : Integer; cdecl;
  Tbson_append_undefined = function(b : Pointer; name : PAnsiChar) : Integer; cdecl;
  Tbson_append_start_object = function(b : Pointer; name : PAnsiChar) : Integer; cdecl;
  Tbson_append_start_array = function(b : Pointer; name : PAnsiChar) : Integer; cdecl;
  Tbson_append_finish_object = function(b : Pointer) : Integer; cdecl;
  Tbson_append_oid = function(b : Pointer; name : PAnsiChar; oid : Pointer) : Integer; cdecl;
  Tbson_append_code_w_scope = function(b : Pointer; name : PAnsiChar; code : PAnsiChar; scope : Pointer) : Integer; cdecl;
  Tbson_append_regex = function(b : Pointer; name : PAnsiChar; pattern : PAnsiChar; options : PAnsiChar) : Integer; cdecl;
  Tbson_append_timestamp2 = function(b : Pointer; name : PAnsiChar; time : Integer; increment : Integer) : Integer; cdecl;
  Tbson_append_binary = function(b : Pointer; name : PAnsiChar; kind : Byte; data : Pointer; len : Integer) : Integer; cdecl;
  Tbson_append_bson = function(b : Pointer; name : PAnsiChar; value : Pointer) : Integer; cdecl;
  Tbson_buffer_size = function(b : Pointer) : Integer; cdecl;
  Tbson_size = function(b : Pointer) : Integer; cdecl;
  Tbson_iterator_create = function: Pointer; cdecl;
  Tbson_iterator_dispose = procedure(i : Pointer); cdecl;
  Tbson_iterator_init = procedure(i : Pointer; b : Pointer); cdecl;
  Tbson_find = function(i : Pointer; b : Pointer; name : PAnsiChar) : TBsonType; cdecl;
  Tbson_iterator_type = function(i : Pointer) : TBsonType; cdecl;
  Tbson_iterator_next = function(i : Pointer) : TBsonType; cdecl;
  Tbson_iterator_key = function(i : Pointer) : PAnsiChar; cdecl;
  Tbson_iterator_double = function(i : Pointer) : Double; cdecl;
  Tbson_iterator_long = function(i : Pointer) : Int64; cdecl;
  Tbson_iterator_int = function(i : Pointer) : Integer; cdecl;
  Tbson_iterator_bool = function(i : Pointer) : Boolean; cdecl;
  Tbson_iterator_string = function(i : Pointer) : PAnsiChar; cdecl;
  Tbson_iterator_date = function(i : Pointer) : Int64; cdecl;
  Tbson_iterator_subiterator = procedure(i : Pointer; sub : Pointer); cdecl;
  Tbson_iterator_oid = function(i : Pointer) : Pointer; cdecl;
  Tbson_iterator_code = function(i : Pointer) : PAnsiChar; cdecl;
  Tbson_iterator_code_scope = procedure(i : Pointer; b : Pointer); cdecl;
  Tbson_iterator_regex = function(i : Pointer) : PAnsiChar; cdecl;
  Tbson_iterator_regex_opts = function(i : Pointer) : PAnsiChar; cdecl;
  Tbson_iterator_timestamp_time = function(i : Pointer) : Integer; cdecl;
  Tbson_iterator_timestamp_increment = function(i : Pointer) : Integer; cdecl;
  Tbson_iterator_bin_len = function(i : Pointer) : Integer; cdecl;
  Tbson_iterator_bin_type = function(i : Pointer) : Byte; cdecl;
  Tbson_iterator_bin_data = function(i : Pointer) : Pointer; cdecl;
var
  mongoLibraryHandle: THandle;
  set_bson_err_handler: Tset_bson_err_handler;
  bson_create: Tbson_create;
  bson_init: Tbson_init;
  bson_destroy: Tbson_destroy;
  bson_dispose: Tbson_dispose;
  bson_copy: Tbson_copy;
  bson_finish: Tbson_finish;
  bson_oid_gen: Tbson_oid_gen;
  bson_oid_to_string: Tbson_oid_to_string;
  bson_oid_from_string: Tbson_oid_from_string;
  bson_append_string: Tbson_append_string;
  bson_append_code: Tbson_append_code;
  bson_append_symbol: Tbson_append_symbol;
  bson_append_int: Tbson_append_int;
  bson_append_long: Tbson_append_long;
  bson_append_double: Tbson_append_double;
  bson_append_date: Tbson_append_date;
  bson_append_bool: Tbson_append_bool;
  bson_append_null: Tbson_append_null;
  bson_append_undefined: Tbson_append_undefined;
  bson_append_start_object: Tbson_append_start_object;
  bson_append_start_array: Tbson_append_start_array;
  bson_append_finish_object: Tbson_append_finish_object;
  bson_append_oid: Tbson_append_oid;
  bson_append_code_w_scope: Tbson_append_code_w_scope;
  bson_append_regex: Tbson_append_regex;
  bson_append_timestamp2: Tbson_append_timestamp2;
  bson_append_binary: Tbson_append_binary;
  bson_append_bson: Tbson_append_bson;
  bson_buffer_size: Tbson_buffer_size;
  bson_size: Tbson_size;
  bson_iterator_create: Tbson_iterator_create;
  bson_iterator_dispose: Tbson_iterator_dispose;
  bson_iterator_init: Tbson_iterator_init;
  bson_find: Tbson_find;
  bson_iterator_type: Tbson_iterator_type;
  bson_iterator_next: Tbson_iterator_next;
  bson_iterator_key: Tbson_iterator_key;
  bson_iterator_double: Tbson_iterator_double;
  bson_iterator_long: Tbson_iterator_long;
  bson_iterator_int: Tbson_iterator_int;
  bson_iterator_bool: Tbson_iterator_bool;
  bson_iterator_string: Tbson_iterator_string;
  bson_iterator_date: Tbson_iterator_date;
  bson_iterator_subiterator: Tbson_iterator_subiterator;
  bson_iterator_oid: Tbson_iterator_oid;
  bson_iterator_code: Tbson_iterator_code;
  bson_iterator_code_scope: Tbson_iterator_code_scope;
  bson_iterator_regex: Tbson_iterator_regex;
  bson_iterator_regex_opts: Tbson_iterator_regex_opts;
  bson_iterator_timestamp_time: Tbson_iterator_timestamp_time;
  bson_iterator_timestamp_increment: Tbson_iterator_timestamp_increment;
  bson_iterator_bin_len: Tbson_iterator_bin_len;
  bson_iterator_bin_type: Tbson_iterator_bin_type;
  bson_iterator_bin_data: Tbson_iterator_bin_data;

  constructor TBsonOID.Create();
  begin
    bson_oid_gen(@value);
  end;

  constructor TBsonOID.Create(s : string);
  begin
    if length(s) <> 24 then
      Raise Exception.Create('Expected a 24 digit hex string');
    bson_oid_from_string(@value, PAnsiChar(AnsiString(s)));
  end;

  constructor TBsonOID.Create(i : TBsonIterator);
  var
     p : PByte;
  begin
    p := bson_iterator_oid(i.handle);
    Move(p^, value, 12);
  end;

  function TBsonOID.AsString() : string;
  var
    buf : array[0..24] of AnsiChar;
  begin
    bson_oid_to_string(@value, @buf);
    Result := string(buf);
  end;

  constructor TBsonIterator.Create();
  begin
    inherited Create();
    handle := bson_iterator_create();
  end;

  constructor TBsonIterator.Create(b : TBson);
  begin
    inherited Create();
    handle := bson_iterator_create();
    bson_iterator_init(handle, b.handle);
  end;

  destructor TBsonIterator.Destroy;
  begin
    bson_iterator_dispose(handle);
  end;

  function TBsonIterator.kind() : TBsonType;
  begin
    Result := bson_iterator_type(handle);
  end;

  function TBsonIterator.next() : Boolean;
  begin
    Result := bson_iterator_next(handle) <> bsonEOO;
  end;

  function TBsonIterator.key() : string;
  begin
    Result := string(System.UTF8ToWideString(bson_iterator_key(handle)));
  end;

  function TBsonIterator.value() : Variant;
    var
      k : TBsonType;
      d : TDateTime;
  begin
    k := kind();
    case k of
      bsonEOO, bsonNULL : Result := Null;
      bsonDOUBLE: Result := bson_iterator_double(handle);
      bsonSTRING, bsonCODE, bsonSYMBOL:
          Result := string(System.UTF8ToWideString(bson_iterator_string(handle)));
      bsonINT: Result := bson_iterator_int(handle);
      bsonBOOL: Result := bson_iterator_bool(handle);
      bsonDATE: begin
           d := Int64toDouble(bson_iterator_date(handle)) / (1000 * 24 * 60 * 60) + 25569;
           Result := d;
      end;
      bsonLONG: Result := bson_iterator_long(handle);
      else
        Raise Exception.Create('BsonType (' + IntToStr(Ord(k)) + ') not supported by TBsonIterator.value');
    end;
  end;

  function TBsonIterator.getOID() : TBsonOID;
  begin
    Result := TBsonOID.Create(Self);
  end;

  function TBsonIterator.getCodeWScope() : TBsonCodeWScope;
  begin
    Result := TBsonCodeWScope.Create(Self);
  end;

  function TBsonIterator.getRegex() : TBsonRegex;
  begin
    Result := TBsonRegex.Create(Self);
  end;

  function TBsonIterator.getTimestamp() : TBsonTimestamp;
  begin
    Result := TBsonTimestamp.Create(Self);
  end;

  function TBsonIterator.getBinary() : TBsonBinary;
  begin
    Result := TBsonBinary.Create(Self);
  end;

  function TBsonIterator.subiterator() : TBsonIterator;
  var
    i : TBsonIterator;
  begin
    i := TBsonIterator.Create();
    bson_iterator_subiterator(handle, i.handle);
    Result := i;
  end;

  function TBsonIterator.getIntegerArray() : TIntegerArray;
    var
      i : TBsonIterator;
      j, count : Integer;
  begin
    if kind() <> bsonArray then
      raise Exception.Create('Iterator does not point to an array');
    i := subiterator();
    count := 0;
    while i.next() do begin
      if i.kind() <> bsonINT then
        raise Exception.Create('Array component is not an Integer');
      inc(count);
    end;
    i := subiterator;
    j := 0;
    SetLength(Result, count);
    while i.next() do begin
      Result[j] := i.value();
      inc(j);
    end;
  end;

  function TBsonIterator.getDoubleArray() : TDoubleArray;
    var
      i : TBsonIterator;
      j, count : Integer;
  begin
    if kind() <> bsonArray then
      raise Exception.Create('Iterator does not point to an array');
    i := subiterator();
    count := 0;
    while i.next() do begin
      if i.kind() <> bsonDOUBLE then
        raise Exception.Create('Array component is not a Double');
      inc(count);
    end;
    i := subiterator;
    j := 0;
    SetLength(Result, count);
    while i.next() do begin
      Result[j] := i.value();
      inc(j);
    end;
  end;

  function TBsonIterator.getStringArray() : TStringArray;
    var
      i : TBsonIterator;
      j, count : Integer;
  begin
    if kind() <> bsonArray then
      raise Exception.Create('Iterator does not point to an array');
    i := subiterator();
    count := 0;
    while i.next() do begin
      if i.kind() <> bsonSTRING then
        raise Exception.Create('Array component is not a string');
      inc(count);
    end;
    i := subiterator;
    j := 0;
    SetLength(Result, count);
    while i.next() do begin
      Result[j] := System.UTF8ToWideString(i.value());
      inc(j);
    end;
  end;

  function TBsonIterator.getBooleanArray() : TBooleanArray;
    var
      i : TBsonIterator;
      j, count : Integer;
  begin
    if kind() <> bsonArray then
      raise Exception.Create('Iterator does not point to an array');
    i := subiterator();
    count := 0;
    while i.next() do begin
      if i.kind() <> bsonBOOL then
        raise Exception.Create('Array component is not a Boolean');
      inc(count);
    end;
    i := subiterator;
    j := 0;
    SetLength(Result, count);
    while i.next() do begin
      Result[j] := i.value();
      inc(j);
    end;
  end;

  function TBson.value(name : string) : Variant;
    var
      i : TBsonIterator;
  begin
    i := find(name);
    try
      if i = nil then
        Result := Null
      else
        Result := i.value;
    finally
      i.Free;
    end;
  end;

  function TBson.iterator() : TBsonIterator;
  begin
    Result := TBsonIterator.Create(Self);
  end;

  constructor TBsonBuffer.Create();
    begin
      inherited Create();
      handle := bson_create();
      bson_init(handle);
    end;

  destructor TBsonBuffer.Destroy();
    begin
      bson_destroy(handle);
      bson_dispose(handle);
      inherited Destroy();
    end;

  function TBsonBuffer.append(name : string; value: PAnsiChar) : Boolean;
  begin
    if handle = nil then
      raise Exception.Create('BsonBuffer already finished');
    Result := (bson_append_string(handle, PAnsiChar(System.UTF8Encode(name)), value) = 0);
  end;

  function TBsonBuffer.appendCode(name : string; value: PAnsiChar) : Boolean;
  begin
    if handle = nil then
      raise Exception.Create('BsonBuffer already finished');
    Result := (bson_append_code(handle, PAnsiChar(System.UTF8Encode(name)), value) = 0);
  end;

  function TBsonBuffer.appendSymbol(name : string; value: PAnsiChar) : Boolean;
  begin
    if handle = nil then
      raise Exception.Create('BsonBuffer already finished');
    Result := (bson_append_symbol(handle, PAnsiChar(System.UTF8Encode(name)), value) = 0);
  end;

  function TBsonBuffer.append(name : string; value: Integer) : Boolean;
  begin
    if handle = nil then
      raise Exception.Create('BsonBuffer already finished');
    Result := (bson_append_int(handle, PAnsiChar(System.UTF8Encode(name)), value) = 0);
  end;

  function TBsonBuffer.append(name : string; value: Int64) : Boolean;
  begin
    if handle = nil then
      raise Exception.Create('BsonBuffer already finished');
    Result := (bson_append_long(handle, PAnsiChar(System.UTF8Encode(name)), value) = 0);
  end;

  function TBsonBuffer.append(name : string; value: Double) : Boolean;
  begin
    if handle = nil then
      raise Exception.Create('BsonBuffer already finished');
    Result := (bson_append_double(handle, PAnsiChar(System.UTF8Encode(name)), value) = 0);
  end;

  function TBsonBuffer.append(name : string; value: TDateTime) : Boolean;
  begin
    if handle = nil then
      raise Exception.Create('BsonBuffer already finished');
    Result := (bson_append_date(handle, PAnsiChar(System.UTF8Encode(name)), Trunc((value - 25569) * 1000 * 60 * 60 * 24)) = 0);
  end;

  function TBsonBuffer.append(name : string; value: Boolean) : Boolean;
  begin
    if handle = nil then
      raise Exception.Create('BsonBuffer already finished');
    Result := (bson_append_bool(handle, PAnsiChar(System.UTF8Encode(name)), value) = 0);
  end;

  function TBsonBuffer.append(name : string; value: TBsonOID) : Boolean;
  begin
    if handle = nil then
      raise Exception.Create('BsonBuffer already finished');
    Result := (bson_append_oid(handle, PAnsiChar(System.UTF8Encode(name)), @value.value) = 0);
  end;

  function TBsonBuffer.append(name : string; value: TBsonCodeWScope) : Boolean;
  begin
    if handle = nil then
      raise Exception.Create('BsonBuffer already finished');
    Result := (bson_append_code_w_scope(handle, PAnsiChar(System.UTF8Encode(name)), PAnsiChar(System.UTF8Encode(value.code)), value.scope.handle) = 0);
  end;

  function TBsonBuffer.append(name : string; value: TBsonRegex) : Boolean;
  begin
    if handle = nil then
      raise Exception.Create('BsonBuffer already finished');
    Result := (bson_append_regex(handle, PAnsiChar(System.UTF8Encode(name)), PAnsiChar(System.UTF8Encode(value.pattern)), PAnsiChar(System.UTF8Encode(value.options))) = 0);
  end;

  function TBsonBuffer.append(name : string; value: TBsonTimestamp) : Boolean;
  begin
    if handle = nil then
      raise Exception.Create('BsonBuffer already finished');
    Result := (bson_append_timestamp2(handle, PAnsiChar(System.UTF8Encode(name)), Trunc((value.time - 25569) * 60 * 60 * 24), value.increment) = 0);
  end;

  function TBsonBuffer.append(name : string; value: TBsonBinary) : Boolean;
  begin
    if handle = nil then
      raise Exception.Create('BsonBuffer already finished');
    Result := (bson_append_binary(handle, PAnsiChar(System.UTF8Encode(name)), value.kind, value.data, value.len) = 0);
  end;

  function TBsonBuffer.append(name : string; value : OleVariant) : Boolean;
    var
      d : double;
      i64 : Int64;
  begin
    case VarType(value) of
      varNull: Result := appendNull(name);
      varInteger: Result := append(name, Integer(value));
      varSingle, varDouble, varCurrency: begin
        d := value;
        Result := append(name, d);
      end;
      varDate: Result := append(name, TDateTime(value));
      varInt64: begin
        i64 := value;
        Result := append(name, i64);
      end;
      varBoolean: Result := append(name, Boolean(value));
      varOleStr: Result := append(name, PAnsiChar(System.UTF8Encode(value)));
    else
      raise Exception.Create('TBson.append(variant): type not supported (' + IntToStr(VarType(value)) + ')');
    end;
  end;

  function TBsonBuffer.appendNull(name : string) : Boolean;
  begin
    if handle = nil then
      raise Exception.Create('BsonBuffer already finished');
    Result := (bson_append_null(handle, PAnsiChar(System.UTF8Encode(name))) = 0);
  end;

  function TBsonBuffer.appendUndefined(name : string) : Boolean;
  begin
    if handle = nil then
      raise Exception.Create('BsonBuffer already finished');
    Result := (bson_append_undefined(handle, PAnsiChar(System.UTF8Encode(name))) = 0);
  end;

  function TBsonBuffer.appendBinary(name : string; kind : Integer; data : Pointer; length : Integer) : Boolean;
  begin
    if handle = nil then
      raise Exception.Create('BsonBuffer already finished');
    Result := (bson_append_binary(handle, PAnsiChar(System.UTF8Encode(name)), kind, data, length) = 0);
  end;

  function TBsonBuffer.append(name : string; value : TBson) : Boolean;
  begin
    Result := (bson_append_bson(handle, PAnsiChar(System.UTF8Encode(name)), value.handle) = 0);
  end;

  function TBsonBuffer.appendArray(name : string; value : array of Integer) : Boolean;
  var
    success : Boolean;
    i, len : Integer;
  begin
    success := (bson_append_start_array(handle, PAnsiChar(System.UTF8Encode(name))) = 0);
    len := Length(value);
    i := 0;
    while success and (i < len) do begin
      success := (bson_append_int(handle, PAnsiChar(AnsiString(IntToStr(i))), value[i]) = 0);
      inc(i);
    end;
    if success then
      success := (bson_append_finish_object(handle) = 0);
    Result := success;
  end;

  function TBsonBuffer.appendArray(name : string; value : array of Double) : Boolean;
  var
    success : Boolean;
    i, len : Integer;
  begin
    success := (bson_append_start_array(handle, PAnsiChar(System.UTF8Encode(name))) = 0);
    len := Length(value);
    i := 0;
    while success and (i < len) do begin
      success := (bson_append_double(handle, PAnsiChar(AnsiString(IntToStr(i))), value[i]) = 0);
      inc(i);
    end;
    if success then
      success := (bson_append_finish_object(handle) = 0);
    Result := success;
  end;

  function TBsonBuffer.appendArray(name : string; value : array of Boolean) : Boolean;
  var
    success : Boolean;
    i, len : Integer;
  begin
    success := (bson_append_start_array(handle, PAnsiChar(System.UTF8Encode(name))) = 0);
    len := Length(value);
    i := 0;
    while success and (i < len) do begin
      success := (bson_append_bool(handle, PAnsiChar(AnsiString(IntToStr(i))), value[i]) = 0);
      inc(i);
    end;
    if success then
      success := (bson_append_finish_object(handle) = 0);
    Result := success;
  end;

  function TBsonBuffer.appendArray(name : string; value : array of string) : Boolean;
  var
    success : Boolean;
    i, len : Integer;
  begin
    success := (bson_append_start_array(handle, PAnsiChar(System.UTF8Encode(name))) = 0);
    len := Length(value);
    i := 0;
    while success and (i < len) do begin
      success := (bson_append_string(handle, PAnsiChar(AnsiString(IntToStr(i))), PAnsiChar(System.UTF8Encode(value[i]))) = 0);
      inc(i);
    end;
    if success then
      success := (bson_append_finish_object(handle) = 0);
    Result := success;
  end;

  function TBsonBuffer.startObject(name : string) : Boolean;
  begin
    if handle = nil then
      raise Exception.Create('BsonBuffer already finished');
    Result := (bson_append_start_object(handle, PAnsiChar(System.UTF8Encode(name))) = 0);
  end;

  function TBsonBuffer.startArray(name : string) : Boolean;
  begin
    if handle = nil then
      raise Exception.Create('BsonBuffer already finished');
    Result := (bson_append_start_array(handle, PAnsiChar(System.UTF8Encode(name))) = 0);
  end;

  function TBsonBuffer.finishObject() : Boolean;
  begin
    if handle = nil then
      raise Exception.Create('BsonBuffer already finished');
    Result := (bson_append_finish_object(handle) = 0);
  end;

  function TBsonBuffer.size() : Integer;
  begin
    if handle = nil then
      raise Exception.Create('BsonBuffer already finished');
    Result := bson_buffer_size(handle);
  end;

  function TBsonBuffer.finish() : TBson;
  begin
    if handle = nil then
      raise Exception.Create('BsonBuffer already finished');
    if bson_finish(handle) = 0 Then begin
        Result := TBson.Create(handle);
        handle := nil;
      end
    else
      Result := nil;
  end;

function TBson.AsString: string;
begin
  Result := '';
  ConvertToString(Result, iterator, 0);
end;

procedure TBson.ConvertToString(var AString: string; i: TBsonIterator; depth: integer);
var
  t : TBsonType;
  j,k : Integer;
  cws : TBsonCodeWScope;
  regex : TBsonRegex;
  ts : TBsonTimestamp;
  bin : TBsonBinary;
  p : PByte;
begin
  while i.next() do
  begin
    t := i.kind();
    if t = bsonEOO then
      break;
    for j:= 1 To depth do
      AString := AString + '    ';
    AString := AString + i.key + ' (' + IntToStr(Ord(t)) + ') : ';
    case t of
        bsonDOUBLE,
        bsonSTRING, bsonSYMBOL, bsonCODE,
        bsonBOOL, bsonDATE, bsonINT, bsonLONG :
            AString := AString + VarToStr(i.value);
        bsonUNDEFINED :
            AString := AString + 'UNDEFINED';
        bsonNULL :
            AString := AString + 'NULL';
        bsonOBJECT, bsonARRAY : begin
            AString := AString + #13#10;
            ConvertToString(AString, i.subiterator, depth+1);
        end;
        bsonOID : AString := AString + i.getOID().AsString();
        bsonCODEWSCOPE : begin
            AString := AString + 'CODEWSCOPE ';
            cws := i.getCodeWScope();
            AString := AString + cws.code + #13#10;
            ConvertToString(AString, cws.scope.iterator, depth+1);
        end;
        bsonREGEX: begin
            regex := i.getRegex();
            AString := AString + regex.pattern + ', ' + regex.options;
        end;
        bsonTIMESTAMP: begin
            ts := i.getTimestamp();
            AString := AString + DateTimeToStr(ts.time) + ' (' + IntToStr(ts.increment) + ')';
        end;
        bsonBINDATA: begin
            bin := i.getBinary();
            AString := AString + 'BINARY (' + IntToStr(bin.kind) + ')';
            p := bin.data;
            for j := 0 to bin.len-1 do begin
              if j and 15 = 0 then begin
                AString := AString + #13#10;
                for k := 1 To depth+1 do
                  AString := AString + '    ';
              end;
              AString := AString + ByteToHex(p^) + ' ';
              Inc(p);
            end;
        end;
    else
      AString := AString + 'UNKNOWN';
    end;
    AString := AString + #13#10;
  end;
end;

  constructor TBson.Create(h : Pointer);
  begin
    handle := h;
    fDS := TDataSet.Create(nil);
  end;

  destructor TBson.Destroy();
    begin
      fDS.Free;
      bson_destroy(handle);
      bson_dispose(handle);
      inherited Destroy();
    end;

  function TBson.size() : Integer;
  begin
    Result := bson_size(handle);
  end;

  function TBson.find(name : string) : TBsonIterator;
  var
    i : TBsonIterator;
  begin
    i := TBsonIterator.Create();
    if bson_find(i.handle, handle, PAnsiChar(System.UTF8Encode(name))) = bsonEOO Then
      i := nil;
    Result := i;
  end;

function TBson.GetFieldDefs: TFieldDefs;

  procedure AddFieldDefs(i: TBsonIterator; depth: integer);
  begin
    while i.next do
    begin
      case i.kind of
        bsonDOUBLE: fDS.FieldDefs.Add(i.key, ftFloat);
        bsonSTRING, bsonSYMBOL, bsonCODE: fDS.FieldDefs.Add(i.key, ftString, 1000);
        bsonBOOL: fDS.FieldDefs.Add(i.key, ftBoolean);
        bsonDATE: fDS.FieldDefs.Add(i.key, ftDateTime);
        bsonINT: fDS.FieldDefs.Add(i.key, ftInteger);
        bsonLONG: fDS.FieldDefs.Add(i.key, ftLargeint);
        bsonOBJECT, bsonARRAY: AddFieldDefs(i.subiterator, depth+1);
        bsonOID: fDS.FieldDefs.Add(i.key, ftString, 12);
        bsonCODEWSCOPE: AddFieldDefs(i.getCodeWScope.scope.iterator, depth+1);
        bsonREGEX: fDS.FieldDefs.Add(i.key, ftString, 1000);
        bsonTIMESTAMP: fDS.FieldDefs.Add(i.key, ftTimeStamp);
        bsonBINDATA: fDS.FieldDefs.Add(i.key, ftBlob);
      end;
    end;
  end;

var
  localIterator: TBsonIterator;
begin
  fDS.FieldDefs.Clear;
  localIterator := iterator;
  try
    AddFieldDefs(localIterator, 0);
  finally
    localIterator.Free;
  end;
  Result := fDS.FieldDefs;
end;

  procedure _display(i : TBsonIterator; depth : Integer);
  var
    t : TBsonType;
    j,k : Integer;
    cws : TBsonCodeWScope;
    regex : TBsonRegex;
    ts : TBsonTimestamp;
    bin : TBsonBinary;
    p : PByte;
  begin
      while i.next() do begin
          t := i.kind();
          if t = bsonEOO then
              break;
          for j:= 1 To depth do
              Write('    ');
          Write(i.key, ' (', Ord(t), ') : ');
          case t of
              bsonDOUBLE,
              bsonSTRING, bsonSYMBOL, bsonCODE,
              bsonBOOL, bsonDATE, bsonINT, bsonLONG :
                  Write(i.value);
              bsonUNDEFINED :
                  Write('UNDEFINED');
              bsonNULL :
                  Write('NULL');
              bsonOBJECT, bsonARRAY : begin
                  Writeln;
                  _display(i.subiterator, depth+1);
              end;
              bsonOID : write(i.getOID().AsString());
              bsonCODEWSCOPE : begin
                  Write('CODEWSCOPE ');
                  cws := i.getCodeWScope();
                  WriteLn(cws.code);
                  _display(cws.scope.iterator, depth+1);
              end;
              bsonREGEX: begin
                  regex := i.getRegex();
                  write(regex.pattern, ', ', regex.options);
              end;
              bsonTIMESTAMP: begin
                  ts := i.getTimestamp();
                  write(DateTimeToStr(ts.time), ' (', ts.increment, ')');
              end;
              bsonBINDATA: begin
                  bin := i.getBinary();
                  Write('BINARY (', bin.kind, ')');
                  p := bin.data;
                  for j := 0 to bin.len-1 do begin
                    if j and 15 = 0 then begin
                      WriteLn;
                      for k := 1 To depth+1 do
                        Write('    ');
                    end;
                    write(ByteToHex(p^), ' ');
                    Inc(p);
                  end;
              end;
          else
              Write('UNKNOWN');
          end;
          Writeln;
      end;
  end;

  procedure TBson.display();
  begin
    if Self = nil then
      WriteLn('nil BSON')
    else
    _display(iterator, 0);
  end;

  constructor TBsonCodeWScope.Create(code_ : string; scope_ : TBson);
  begin
    code := code_;
    scope := scope_;
  end;

  constructor TBsonCodeWScope.Create(i : TBsonIterator);
  var
    b, c : Pointer;
  begin
    code := string(bson_iterator_code(i.handle));
    b := bson_create();
    bson_iterator_code_scope(i.handle, b);
    c := bson_create();
    bson_copy(c, b);
    scope := TBson.Create(c);
    bson_dispose(b);
  end;

  constructor TBsonRegex.Create(pattern_ : string; options_ : string);
  begin
    pattern := pattern_;
    options := options_;
  end;

  constructor TBsonRegex.Create(i : TBsonIterator);
  begin
     pattern := string(bson_iterator_regex(i.handle));
     options := string(bson_iterator_regex_opts(i.handle));
  end;


  constructor TBsonTimestamp.Create(time_ : TDateTime; increment_ : Integer);
  begin
    time := time_;
    increment := increment_;
  end;

  constructor TBsonTimestamp.Create(i : TBsonIterator);
  begin
    time := bson_iterator_timestamp_time(i.handle) / (60.0 * 60 * 24) + 25569;
    increment := bson_iterator_timestamp_increment(i.handle);
  end;

  constructor TBsonBinary.Create(p: Pointer; length: Integer);
  begin
    GetMem(data, length);
    Move(p^, data^, length);
    kind := 0;
  end;

  constructor TBsonBinary.Create(i : TBsonIterator);
  var
    p : Pointer;
  begin
    kind := bson_iterator_bin_type(i.handle);
    len := bson_iterator_bin_len(i.handle);
    p := bson_iterator_bin_data(i.handle);
    GetMem(data, len);
    Move(p^, data^, len);
  end;

  destructor TBsonBinary.Destroy;
  begin
    FreeMem(data);
  end;

  function ByteToHex(InByte : Byte) : string;
  const digits : array[0..15] of Char = '0123456789ABCDEF';
  begin
    result := digits[InByte shr 4] + digits[InByte and $0F];
  end;

  function BSON(x : array of OleVariant) : TBson;
  var
    len   : Integer;
    i     : Integer;
    bb    : TBsonBuffer;
    depth : Integer;
    key   : string;
    value : string;
  begin
    bb := TBsonBuffer.Create();
    len := Length(x);
    i := 0;
    depth := 0;
    while i < len do begin
      key := VarToStr(x[i]);
      if key = '}' then begin
        if depth = 0 then
          Raise Exception.Create('BSON(): unexpected "}"');
        bb.finishObject();
        dec(depth);
      end
      else begin
        inc(i);
        if i = Len then
          raise Exception.Create('BSON(): expected value for ' + key);
        value := VarToStr(x[i]);
        if value = '{' then begin
          bb.startObject(key);
          inc(depth);
        end
        else
          bb.append(key, x[i]);
      end;
      inc(i);
    end;
    if depth > 0 then
      Raise Exception.Create('BSON: open subobject');
    Result := bb.finish();
  end;

  procedure err_handler(msg : PAnsiChar);
  begin
    Raise Exception.Create(string(msg));
  end;

procedure LoadMongoLibrary;
begin
  mongoLibraryHandle := 0;

  if FileExists('mongoc.dll') then
  begin
    mongoLibraryHandle := LoadLibrary('mongoc.dll');

    if mongoLibraryHandle > 0 then
    begin
      @set_bson_err_handler := GetProcAddress(mongoLibraryHandle, 'set_bson_err_handler');
      @bson_create := GetProcAddress(mongoLibraryHandle, 'bson_create');
      @bson_init := GetProcAddress(mongoLibraryHandle, 'bson_init');
      @bson_destroy := GetProcAddress(mongoLibraryHandle, 'bson_destroy');
      @bson_dispose := GetProcAddress(mongoLibraryHandle, 'bson_dispose');
      @bson_copy := GetProcAddress(mongoLibraryHandle, 'bson_copy');
      @bson_finish := GetProcAddress(mongoLibraryHandle, 'bson_finish');
      @bson_oid_gen := GetProcAddress(mongoLibraryHandle, 'bson_oid_gen');
      @bson_oid_to_string := GetProcAddress(mongoLibraryHandle, 'bson_oid_to_string');
      @bson_oid_from_string := GetProcAddress(mongoLibraryHandle, 'bson_oid_from_string');
      @bson_append_string := GetProcAddress(mongoLibraryHandle, 'bson_append_string');
      @bson_append_code := GetProcAddress(mongoLibraryHandle, 'bson_append_code');
      @bson_append_symbol := GetProcAddress(mongoLibraryHandle, 'bson_append_symbol');
      @bson_append_int := GetProcAddress(mongoLibraryHandle, 'bson_append_int');
      @bson_append_long := GetProcAddress(mongoLibraryHandle, 'bson_append_long');
      @bson_append_double := GetProcAddress(mongoLibraryHandle, 'bson_append_double');
      @bson_append_date := GetProcAddress(mongoLibraryHandle, 'bson_append_date');
      @bson_append_bool := GetProcAddress(mongoLibraryHandle, 'bson_append_bool');
      @bson_append_null := GetProcAddress(mongoLibraryHandle, 'bson_append_null');
      @bson_append_undefined := GetProcAddress(mongoLibraryHandle, 'bson_append_undefined');
      @bson_append_start_object := GetProcAddress(mongoLibraryHandle, 'bson_append_start_object');
      @bson_append_start_array := GetProcAddress(mongoLibraryHandle, 'bson_append_start_array');
      @bson_append_finish_object := GetProcAddress(mongoLibraryHandle, 'bson_append_finish_object');
      @bson_append_oid := GetProcAddress(mongoLibraryHandle, 'bson_append_oid');
      @bson_append_code_w_scope := GetProcAddress(mongoLibraryHandle, 'bson_append_code_w_scope');
      @bson_append_regex := GetProcAddress(mongoLibraryHandle, 'bson_append_regex');
      @bson_append_timestamp2 := GetProcAddress(mongoLibraryHandle, 'bson_append_timestamp2');
      @bson_append_binary := GetProcAddress(mongoLibraryHandle, 'bson_append_binary');
      @bson_append_bson := GetProcAddress(mongoLibraryHandle, 'bson_append_bson');
      @bson_buffer_size := GetProcAddress(mongoLibraryHandle, 'bson_buffer_size');
      @bson_size := GetProcAddress(mongoLibraryHandle, 'bson_size');
      @bson_iterator_create := GetProcAddress(mongoLibraryHandle, 'bson_iterator_create');
      @bson_iterator_dispose := GetProcAddress(mongoLibraryHandle, 'bson_iterator_dispose');
      @bson_iterator_init := GetProcAddress(mongoLibraryHandle, 'bson_iterator_init');
      @bson_find := GetProcAddress(mongoLibraryHandle, 'bson_find');
      @bson_iterator_type := GetProcAddress(mongoLibraryHandle, 'bson_iterator_type');
      @bson_iterator_next := GetProcAddress(mongoLibraryHandle, 'bson_iterator_next');
      @bson_iterator_key := GetProcAddress(mongoLibraryHandle, 'bson_iterator_key');
      @bson_iterator_double := GetProcAddress(mongoLibraryHandle, 'bson_iterator_double');
      @bson_iterator_long := GetProcAddress(mongoLibraryHandle, 'bson_iterator_long');
      @bson_iterator_int := GetProcAddress(mongoLibraryHandle, 'bson_iterator_int');
      @bson_iterator_bool := GetProcAddress(mongoLibraryHandle, 'bson_iterator_bool');
      @bson_iterator_string := GetProcAddress(mongoLibraryHandle, 'bson_iterator_string');
      @bson_iterator_date := GetProcAddress(mongoLibraryHandle, 'bson_iterator_date');
      @bson_iterator_subiterator := GetProcAddress(mongoLibraryHandle, 'bson_iterator_subiterator');
      @bson_iterator_oid := GetProcAddress(mongoLibraryHandle, 'bson_iterator_oid');
      @bson_iterator_code := GetProcAddress(mongoLibraryHandle, 'bson_iterator_code');
      @bson_iterator_code_scope := GetProcAddress(mongoLibraryHandle, 'bson_iterator_code_scope');
      @bson_iterator_regex := GetProcAddress(mongoLibraryHandle, 'bson_iterator_regex');
      @bson_iterator_regex_opts := GetProcAddress(mongoLibraryHandle, 'bson_iterator_regex_opts');
      @bson_iterator_timestamp_time := GetProcAddress(mongoLibraryHandle, 'bson_iterator_timestamp_time');
      @bson_iterator_timestamp_increment := GetProcAddress(mongoLibraryHandle, 'bson_iterator_timestamp_increment');
      @bson_iterator_bin_len := GetProcAddress(mongoLibraryHandle, 'bson_iterator_bin_len');
      @bson_iterator_bin_type := GetProcAddress(mongoLibraryHandle, 'bson_iterator_bin_type');
      @bson_iterator_bin_data := GetProcAddress(mongoLibraryHandle, 'bson_iterator_bin_data');

      @Int64toDouble := GetProcAddress(mongoLibraryHandle, 'bson_int64_to_double');

      bsonEmpty := BSON([]);
      set_bson_err_handler(Addr(err_handler));
    end;
  end;
end;

procedure UnloadMongoLibrary;
begin
  if mongoLibraryHandle > 0 then
  begin
    FreeLibrary(mongoLibraryHandle);
  end;
end;

initialization
  LoadMongoLibrary;

finalization
  UnloadMongoLibrary;
end.

