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
{ GridFS Unit - The classes in this unit are used to store and/or
  access a "Grid File System" (GridFS) on a MongoDB server.
  While primarily intended to store large documents that
  won't fit on the server as a single BSON object,
  GridFS may also be used to store large numbers of smaller files.

  See http://www.mongodb.org/display/DOCS/GridFS and
  http://www.mongodb.org/display/DOCS/When+to+use+GridFS.

  Objects of class TGridFS represent the interface to the GridFS.
  Objects of class TGridfile are used to access gridfiles and read from them.
  Objects of class TGridfileWriter are used to write buffered data to the GridFS.
}
unit GridFS;

interface
  Uses
    MongoDB, MongoBson;

  type
    TGridfile = class;
    TGridfileWriter = class;

    TGridFS = class(TObject)
    private
      var
        { Pointer to externally managed data representing the GridFS }
        handle : Pointer;
        { Holds a reference to the TMongo object used in construction.
          Prevents release until the TGridFS is destroyed. }
        conn   : TMongo;
    public
      { Create a TGridFS object for accessing the GridFS on the MongoDB server.
        Parameter mongo is an already established connection object to the
        server; db is the name of the database in which to construct the GridFS.
        The prefix defaults to 'fs'.}
      constructor Create(mongo : TMongo; db : string); overload;
      { Create a TGridFS object for accessing the GridFS on the MongoDB server.
        Parameter mongo is an already established connection object to the
        server; db is the name of the database in which to construct the GridFS.
        prefix is appended to the database name for the collections that represent
        the GridFS: 'db.prefix.files' & 'db.prefix.chunks'. }
      constructor Create(mongo : TMongo; db : string; prefix : string); overload;
      { Store a file on the GridFS.  filename is the path to the file.
        Returns True if successful; otherwise, False. }
      function storeFile(filename : string) : Boolean; overload;
      { Store a file on the GridFS.  filename is the path to the file.
        remoteName is the name that the file will be known as within the GridFS.
        Returns True if successful; otherwise, False. }
      function storeFile(filename : string; remoteName : string) : Boolean; overload;
      { Store a file on the GridFS.  filename is the path to the file.
        remoteName is the name that the file will be known as within the GridFS.
        contentType is the MIME-type content type of the file.
        Returns True if successful; otherwise, False. }
      function storeFile(filename : string; remoteName : string; contentType : string) : Boolean; overload;
      { Remove a file from the GridFS. }
      procedure removeFile(remoteName : string);
      { Store data as a GridFS file.  Pointer is the address of the data and length
        is its size. remoteName is the name that the file will be known as within the GridFS.
        Returns True if successful; otherwise, False. }
      function store(p : Pointer; length : Int64; remoteName : string) : Boolean; overload;
      { Store data as a GridFS file.  Pointer is the address of the data and length
        is its size. remoteName is the name that the file will be known as within the GridFS.
        contentType is the MIME-type content type of the file.
        Returns True if successful; otherwise, False. }
      function store(p : Pointer; length : Int64; remoteName : string; contentType : string) : Boolean; overload;
      { Create a TGridfileWriter object for writing buffered data to a GridFS file.
        remoteName is the name that the file will be known as within the GridFS. }
      function writerCreate(remoteName : string) : TGridfileWriter; overload;
      { Create a TGridfileWriter object for writing buffered data to a GridFS file.
        remoteName is the name that the file will be known as within the GridFS.
        contentType is the MIME-type content type of the file. }
      function writerCreate(remoteName : string; contentType : string) : TGridfileWriter; overload;
      { Locate a GridFS file by its remoteName and return a TGridfile object for
        accessing it. }
      function find(remoteName : string) : TGridfile; overload;
      { Locate a GridFS file by an TBson query document on the GridFS file descriptors.
        Returns a TGridfile object for accessing it. }
      function find(query : TBson) : TGridfile; overload;
      { Destroy this GridFS object.  Releases external resources. }
      destructor Destroy(); override;
    end;

    {  Objects of class TGridfile are used to access gridfiles and read from them. }
    TGridfile = class(TObject)
    private
      var
        { Pointer to externally managed data representing the gridfile }
        handle : Pointer;
        { Hold a reference to the TGridFS object used in construction of this
          TGridfile.  Prevents release until this TGridfile is destroyed. }
        gfs : TGridFS;
        { Create a TGridfile object.  Internal use only by TGridFS.find(). }
      constructor Create(gridfs : TGridFS);
    public
      { Get the filename (remoteName) of this gridfile. }
      function getFilename() : string;
      { Get the size of the chunks into which the file is divided. }
      function getChunkSize() : Integer;
      { Get the length of this gridfile. }
      function getLength() : Int64;
      { Get the content type of this gridfile. }
      function getContentType() : string;
      { Get the upload date of this gridfile. }
      function getUploadDate() : TDateTime;
      { Get the MD5 hash of this gridfile.  This is a 16-digit hex string. }
      function getMD5() : string;
      { Get any metadata associated with this gridfile as a TBson document.
        Returns nil if there is none. }
      function getMetadata() : TBson;
      { Get the number of chunks into which the file is divided. }
      function getChunkCount() : Integer;
      { Get the descriptor of this gridfile as a TBson document. }
      function getDescriptor() : TBson;
      { Get the Ith chunk of this gridfile.  The content of the chunk is
        in the 'data' field of the returned TBson document.  Returns nil
        if i is not in the range 0 to getChunkCount() - 1. }
      function getChunk(i : Integer) : TBson;
      { Get a cursor for stepping through a range of chunks of this gridfile.
        i is the index of the first chunk to be returned.  count is the number
        of chunks to return.  Returns nil if there are no chunks in the
        specified range. }
      function getChunks(i : Integer; count : Integer) : TMongoCursor;
      { Read data from this gridfile.  The gridfile maintains a current position
        so that successive reads will return consecutive data. The data is
        read to the address indicated by p and length bytes are read.  The size
        of the data read is returned and can be less than length if there was
        not enough data remaining to be read. }
      function read(p : Pointer; length : Int64) : Int64;
      { Seek to a specified offset within the gridfile.  read() will then
        return data starting at that location.  Returns the position that
        was set.  This can be at the end of the gridfile if offset is greater
        the length of this gridfile. }
      function seek(offset : Int64) : Int64;
      { Destroy this TGridfile object.  Releases external resources. }
      destructor Destroy(); override;
    end;

    { Objects of class TGridfileWriter are used to write buffered data to the GridFS. }
    TGridfileWriter = class(TObject)
    private
      var
        { Holds a pointer to externally managed data representing the TGridfileWriter. }
        handle : Pointer;
        { Holds a reference to the TGridFS object used in construction.
          Prevents release of the TGridFS until this TGridfileWriter is destroyed. }
        gfs    : TGridFS;
    public
      { Create a TGridfile writer on the given TGridFS that will write data to
        the given remoteName. }
      constructor Create(gridfs : TGridFS; remoteName : string); overload;
      { Create a TGridfile writer on the given TGridFS that will write data to
        the given remoteName. contentType is the MIME-type content type of the gridfile
        to be written. }
      constructor Create(gridfs : TGridFS; remoteName : string; contentType : string); overload;
      { Write data to this TGridfileWriter. p is the address of the data and length
        is its size. Multiple calls to write() may be made to append successive
        data. }
      procedure write(p : Pointer; length : Int64);
      { Finish with this TGridfileWriter.  Flushes any data remaining to be written
        to a chunk and posts the 'directory' information of the gridfile to the
        GridFS. Returns True if successful; otherwise, False. }
      function finish() : Boolean;
      { Destroy this TGridfileWriter.  Calls finish() if necessary and releases
        external resources. }
      destructor Destroy(); override;
    end;

implementation
  uses
    Windows,
    SysUtils;

type
  Tgridfs_create = function: Pointer; cdecl;
  Tgridfs_dispose = procedure(g : Pointer); cdecl;
  Tgridfs_init = function(c : Pointer; db : PAnsiChar; prefix : PAnsiChar; g : Pointer) : Integer; cdecl;
  Tgridfs_destroy = procedure(g : Pointer); cdecl;
  Tgridfs_store_file = function(g : Pointer; filename : PAnsiChar; remoteName : PAnsiChar; contentType : PAnsiChar) : Integer; cdecl;
  Tgridfs_remove_filename = procedure(g : Pointer; remoteName : PAnsiChar); cdecl;
  Tgridfs_store_buffer = function(g : Pointer; p : Pointer; size : Int64; remoteName : PAnsiChar; contentType : PAnsiChar) : Integer; cdecl;
  Tgridfile_create = function: Pointer; cdecl;
  Tgridfile_dispose = procedure(gf : Pointer); cdecl;
  Tgridfile_writer_init = procedure(gf : Pointer; gfs : Pointer; remoteName : PAnsiChar; contentType : PAnsiChar); cdecl;
  Tgridfile_write_buffer = procedure(gf : Pointer; data : Pointer; length : Int64); cdecl;
  Tgridfile_writer_done = function(gf : Pointer) : Integer; cdecl;
  Tgridfs_find_query = function(g : Pointer; query : Pointer; gf : Pointer) : Integer; cdecl;
  Tgridfile_destroy = procedure(gf : Pointer); cdecl;
  Tgridfile_get_filename = function(gf : Pointer) : PAnsiChar; cdecl;
  Tgridfile_get_chunksize = function(gf : Pointer) : Integer; cdecl;
  Tgridfile_get_contentlength = function(gf : Pointer) : Int64; cdecl;
  Tgridfile_get_contenttype = function(gf : Pointer) : PAnsiChar; cdecl;
  Tgridfile_get_uploaddate = function(gf : Pointer) : Int64; cdecl;
  Tgridfile_get_md5 = function(gf : Pointer) : PAnsiChar; cdecl;
  Tgridfile_get_metadata = procedure(gf : Pointer; b : Pointer); cdecl;
  Tbson_create = function: Pointer; cdecl;
  Tbson_dispose = procedure(b : Pointer); cdecl;
  Tbson_size = function(b : Pointer) : Integer; cdecl;
  Tbson_copy = procedure(dest : Pointer; src : Pointer); cdecl;
  Tgridfile_get_numchunks = function(gf : Pointer) : Integer; cdecl;
  Tgridfile_get_descriptor = procedure(gf : Pointer; b : Pointer); cdecl;
  Tgridfile_get_chunk = procedure(gf : Pointer; i : Integer; b : Pointer); cdecl;
  Tgridfile_get_chunks = function(gf : Pointer; i : Integer; count : Integer) : Pointer; cdecl;
  Tgridfile_read = function(gf : Pointer; size : Int64; buf : Pointer) : Int64; cdecl;
  Tgridfile_seek = function(gf : Pointer; offset : Int64) : Int64; cdecl;

var
  mongoLibraryHandle: THandle;
  gridfs_create: Tgridfs_create;
  gridfs_dispose: Tgridfs_dispose;
  gridfs_init: Tgridfs_init;
  gridfs_destroy: Tgridfs_destroy;
  gridfs_store_file: Tgridfs_store_file;
  gridfs_remove_filename: Tgridfs_remove_filename;
  gridfs_store_buffer: Tgridfs_store_buffer;
  gridfile_create: Tgridfile_create;
  gridfile_dispose: Tgridfile_dispose;
  gridfile_writer_init: Tgridfile_writer_init;
  gridfile_write_buffer: Tgridfile_write_buffer;
  gridfile_writer_done: Tgridfile_writer_done;
  gridfs_find_query: Tgridfs_find_query;
  gridfile_destroy: Tgridfile_destroy;
  gridfile_get_filename: Tgridfile_get_filename;
  gridfile_get_chunksize: Tgridfile_get_chunksize;
  gridfile_get_contentlength: Tgridfile_get_contentlength;
  gridfile_get_contenttype: Tgridfile_get_contenttype;
  gridfile_get_uploaddate: Tgridfile_get_uploaddate;
  gridfile_get_md5: Tgridfile_get_md5;
  gridfile_get_metadata: Tgridfile_get_metadata;
  bson_create: Tbson_create;
  bson_dispose: Tbson_dispose;
  bson_size: Tbson_size;
  bson_copy: Tbson_copy;
  gridfile_get_numchunks: Tgridfile_get_numchunks;
  gridfile_get_descriptor: Tgridfile_get_descriptor;
  gridfile_get_chunk: Tgridfile_get_chunk;
  gridfile_get_chunks: Tgridfile_get_chunks;
  gridfile_read: Tgridfile_read;
  gridfile_seek: Tgridfile_seek;
  
  constructor TGridFS.Create(mongo: TMongo; db: string; prefix : string);
  begin
    conn := mongo;
    handle := gridfs_create();
    if gridfs_init(mongo.handle, PAnsiChar(System.UTF8Encode(db)),
                                 PAnsiChar(System.UTF8Encode(prefix)), handle) <> 0 then begin
       gridfs_dispose(handle);
       Raise Exception.Create('Unable to create GridFS');
    end;
  end;

  constructor TGridFS.Create(mongo: TMongo; db: string);
  begin
    Create(mongo, db, 'fs');
  end;

  destructor TGridFS.Destroy();
  begin
    gridfs_destroy(handle);
    gridfs_dispose(handle);
  end;

  function TGridFS.storeFile(filename : string; remoteName : string; contentType : string) : Boolean;
  begin
    Result := (gridfs_store_file(handle, PAnsiChar(System.UTF8Encode(filename)),
                                         PAnsiChar(System.UTF8Encode(remoteName)),
                                         PAnsiChar(System.UTF8Encode(contentType))) = 0);
  end;

  function TGridFS.storeFile(filename : string; remoteName : string) : Boolean;
  begin
    Result := storeFile(filename, remoteName, '');
  end;

  function TGridFS.storeFile(filename : string) : Boolean;
  begin
    Result := storeFile(filename, filename, '');
  end;

  procedure TGridFS.removeFile(remoteName : string);
  begin
    gridfs_remove_filename(handle, PAnsiChar(System.UTF8Encode(remoteName)));
  end;

  function TGridFS.store(p : Pointer; length : Int64; remoteName : string; contentType : string) : Boolean;
  begin
    Result := (gridfs_store_buffer(handle, p, length, PAnsiChar(System.UTF8Encode(remoteName)),
                                                      PAnsiChar(System.UTF8Encode(contentType))) = 0);
  end;

  function TGridFS.store(p : Pointer; length : Int64; remoteName : string) : Boolean;
  begin
    Result := store(p, length, remoteName, '');
  end;

  function TGridFS.writerCreate(remoteName : string; contentType : string) : TGridfileWriter;
  begin
    Result := TGridfileWriter.Create(Self, remoteName, contentType);
  end;

  function TGridFS.writerCreate(remoteName : string) : TGridfileWriter;
  begin
    Result := writerCreate(remoteName, '');
  end;

  constructor TGridfileWriter.Create(gridfs : TGridFS; remoteName : string; contentType : string);
  begin
    gfs := gridfs;
    handle := gridfile_create();
    gridfile_writer_init(handle, gridfs.handle, PAnsiChar(System.UTF8Encode(remoteName)), PAnsiChar(System.UTF8Encode(contentType)));
  end;

  constructor TGridfileWriter.Create(gridfs : TGridFS; remoteName : string);
  begin
    Create(gridfs, remoteName, '');
  end;

  procedure TGridfileWriter.write(p: Pointer; length: Int64);
  begin
    gridfile_write_buffer(handle, p, length);
  end;

  function TGridfileWriter.finish() : Boolean;
  begin
    if handle = nil then
      Result := True
    else begin
      Result := (gridfile_writer_done(handle) = 0);
      gridfile_dispose(handle);
      handle := nil;
    end;
  end;

  destructor TGridfileWriter.Destroy();
  begin
    finish();
  end;

  function TGridFS.find(query : TBson) : TGridfile;
  var
    gf : TGridfile;
  begin
    gf := TGridfile.Create(Self);
    if gridfs_find_query(handle, query.handle, gf.handle) = 0 then
      Result := gf
    else begin
      gf.Free;
      Result := nil;
    end;
  end;

  function TGridFS.find(remoteName : string) : TGridfile;
  begin
    Result := find(BSON(['filename', System.UTF8Encode(remoteName)]));
  end;

  constructor TGridfile.Create(gridfs : TGridFS);
  begin
    gfs := gridfs;
    handle := gridfile_create();
  end;

  destructor TGridfile.Destroy();
  begin
    if handle <> nil then begin
      gridfile_destroy(handle);
      gridfile_dispose(handle);
      handle := nil;
    end;
  end;

  function TGridfile.getFilename() : string;
  begin
    Result := string(System.UTF8ToWideString(gridfile_get_filename(handle)));
  end;

  function TGridfile.getChunkSize() : Integer;
  begin
    Result := gridfile_get_chunksize(handle);
  end;

  function TGridfile.getLength() : Int64;
  begin
    Result := gridfile_get_contentlength(handle);
  end;

  function TGridfile.getContentType() : string;
  begin
    Result := string(System.UTF8ToWideString(gridfile_get_contenttype(handle)));
  end;

  function TGridfile.getUploadDate() : TDateTime;
  begin
    Result := Int64toDouble(gridfile_get_uploaddate(handle)) / (1000 * 24 * 60 * 60) + 25569;
  end;

  function TGridfile.getMD5() : string;
  begin
    Result := string(gridfile_get_md5(handle));
  end;

  function TGridfile.getMetadata() : TBson;
  var
    b : Pointer;
    res : TBson;
  begin
    b := bson_create();
    gridfile_get_metadata(handle, b);
    if bson_size(b) <= 5 then
      Result := nil
    else begin
      res := TBson.Create(bson_create());
      bson_copy(res.handle, b);
      Result := res;
    end;
    bson_dispose(b);
  end;

  function TGridfile.getChunkCount() : Integer;
  begin
    Result := gridfile_get_numchunks(handle);
  end;

  function TGridfile.getDescriptor() : TBson;
  var
    b : Pointer;
    res : TBson;
  begin
    b := bson_create();
    gridfile_get_descriptor(handle, b);
    res := TBson.Create(bson_create());
    bson_copy(res.handle, b);
    bson_dispose(b);
    Result := res;
  end;

  function TGridfile.getChunk(i : Integer) : TBson;
  var
    b : TBson;
  begin
    b := TBson.Create(bson_create());
    gridfile_get_chunk(handle, i, b.handle);
    if b.size() <= 5 then
    begin
      b.Free;
      Result := nil;
    end
    else
      Result := b;
  end;

  function TGridfile.getChunks(i : Integer; count : Integer) : TMongoCursor;
  var
    cursor : TMongoCursor;
  begin
    cursor := TMongoCursor.Create();
    cursor.handle := gridfile_get_chunks(handle, i, count);
    if cursor.handle = nil then
    begin
      cursor.free;
      Result := nil;
    end
    else
      Result := cursor;
  end;

  function TGridfile.read(p : Pointer; length : Int64) : Int64;
  begin
    Result := gridfile_read(handle, length, p);
  end;

  function TGridfile.seek(offset : Int64) : Int64;
  begin
    Result := gridfile_seek(handle, offset);
  end;

procedure LoadMongoLibrary;
begin
  mongoLibraryHandle := 0;

  if FileExists('mongoc.dll') then
  begin
    mongoLibraryHandle := LoadLibrary('mongoc.dll');

    if mongoLibraryHandle > 0 then
    begin
      @gridfs_create := GetProcAddress(mongoLibraryHandle, 'gridfs_create');
      @gridfs_dispose := GetProcAddress(mongoLibraryHandle, 'gridfs_dispose');
      @gridfs_init := GetProcAddress(mongoLibraryHandle, 'gridfs_init');
      @gridfs_destroy := GetProcAddress(mongoLibraryHandle, 'gridfs_destroy');
      @gridfs_store_file := GetProcAddress(mongoLibraryHandle, 'gridfs_store_file');
      @gridfs_remove_filename := GetProcAddress(mongoLibraryHandle, 'gridfs_remove_filename');
      @gridfs_store_buffer := GetProcAddress(mongoLibraryHandle, 'gridfs_store_buffer');
      @gridfile_create := GetProcAddress(mongoLibraryHandle, 'gridfile_create');
      @gridfile_dispose := GetProcAddress(mongoLibraryHandle, 'gridfile_dispose');
      @gridfile_writer_init := GetProcAddress(mongoLibraryHandle, 'gridfile_writer_init');
      @gridfile_write_buffer := GetProcAddress(mongoLibraryHandle, 'gridfile_write_buffer');
      @gridfile_writer_done := GetProcAddress(mongoLibraryHandle, 'gridfile_writer_done');
      @gridfs_find_query := GetProcAddress(mongoLibraryHandle, 'gridfs_find_query');
      @gridfile_destroy := GetProcAddress(mongoLibraryHandle, 'gridfile_destroy');
      @gridfile_get_filename := GetProcAddress(mongoLibraryHandle, 'gridfile_get_filename');
      @gridfile_get_chunksize := GetProcAddress(mongoLibraryHandle, 'gridfile_get_chunksize');
      @gridfile_get_contentlength := GetProcAddress(mongoLibraryHandle, 'gridfile_get_contentlength');
      @gridfile_get_contenttype := GetProcAddress(mongoLibraryHandle, 'gridfile_get_contenttype');
      @gridfile_get_uploaddate := GetProcAddress(mongoLibraryHandle, 'gridfile_get_uploaddate');
      @gridfile_get_md5 := GetProcAddress(mongoLibraryHandle, 'gridfile_get_md5');
      @gridfile_get_metadata := GetProcAddress(mongoLibraryHandle, 'gridfile_get_metadata');
      @bson_create := GetProcAddress(mongoLibraryHandle, 'bson_create');
      @bson_dispose := GetProcAddress(mongoLibraryHandle, 'bson_dispose');
      @bson_size := GetProcAddress(mongoLibraryHandle, 'bson_size');
      @bson_copy := GetProcAddress(mongoLibraryHandle, 'bson_copy');
      @gridfile_get_numchunks := GetProcAddress(mongoLibraryHandle, 'gridfile_get_numchunks');
      @gridfile_get_descriptor := GetProcAddress(mongoLibraryHandle, 'gridfile_get_descriptor');
      @gridfile_get_chunk := GetProcAddress(mongoLibraryHandle, 'gridfile_get_chunk');
      @gridfile_get_chunks := GetProcAddress(mongoLibraryHandle, 'gridfile_get_chunks');
      @gridfile_read := GetProcAddress(mongoLibraryHandle, 'gridfile_read');
      @gridfile_seek := GetProcAddress(mongoLibraryHandle, 'gridfile_seek');
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
