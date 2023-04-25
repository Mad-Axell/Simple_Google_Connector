using System;
using System.IO;
using System.Net;
using System.Web;
using System.Linq;
using System.Threading.Tasks;
using System.Net.Http.Headers;
using System.Collections.Generic;
using Newtonsoft.Json;

using Google.Apis.Drive.v3;
using Google.Apis.Drive.v3.Data;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Services;
using Google.Apis.Http;
using Google.Apis.Download;
using Logger;
using System.Threading;

namespace GoogleConnector
{
     public class GoogleDriveObject
    {
        #region Fields
        private int _sleeptime;
        private string _applicationname;
        private string[] _scopes;
        private GoogleCredential _credential;
        private DriveService _driveService;
        #endregion

        #region Props
        public int SleepTime
        {
            set
            {
                this._sleeptime = value;
                if (Log.logLevel >= LogLevel.Trace)
                {
                    Log.log(Logger.LogLevel.Trace, string.Format("For Int32 SleepTime set value {0}", value));
                }
            }
            get
            {
                return this._sleeptime;
            }
        }
        public string ApplicationName
        {
            set
            {
                this._applicationname = value;
                if (Log.logLevel >= LogLevel.Trace)
                {
                    Log.log(Logger.LogLevel.Trace, string.Format("For string ApplicationName set value {0}", value));
                }
            }
            get
            {
                return this._applicationname;
            }
        }
        public string[] Scopes
        {
            set
            {
                this._scopes = value;
                if (Log.logLevel >= LogLevel.Trace)
                {
                    Log.log(Logger.LogLevel.Trace, string.Format("For string[] Scopes set value - {0}", JsonConvert.SerializeObject(_scopes)));
                }
            }
            get
            {
                return this._scopes;
            }
        }
        public GoogleCredential credential
        {
            set
            {
                this._credential = value;
                if (Log.logLevel >= LogLevel.Trace)
                {
                    Log.log(Logger.LogLevel.Trace, string.Format("For GoogleCredential credential set value - {0}", value.ToString()));
                }
            }
            get
            {
                return this._credential;
            }
        }
        public DriveService driveService
        {
            set
            {
                this._driveService = value;
                if (Log.logLevel >= LogLevel.Trace)
                {
                    Log.log(Logger.LogLevel.Trace, string.Format("For SheetsService service set value - {0}", value.ToString()));
                }
            }
            get
            {
                return this._driveService;
            }
        }
        #endregion

        #region Constructor
        public GoogleDriveObject(string Credential, string applicationname, int SleepTime)
        {
            this.SleepTime = SleepTime;
            this.ApplicationName = applicationname;
            this.Scopes = new string[]
            {
                DriveService.Scope.Drive,
                DriveService.Scope.DriveFile,
                DriveService.Scope.DriveAppdata,
                DriveService.Scope.DriveMetadata,
                DriveService.Scope.DrivePhotosReadonly,
                DriveService.Scope.DriveReadonly
            };

            this.credential = GoogleCredential.FromJson(Credential).CreateScoped(Scopes);

            this.driveService = new DriveService(new BaseClientService.Initializer()
            {
                HttpClientInitializer = credential,
                ApplicationName = ApplicationName,
            });

            if (Log.logLevel >= LogLevel.Trace)
            {
                Log.log(Logger.LogLevel.Trace, string.Format("GoogleDriveObject was successfully created"));
            }
        }
        #endregion

        #region Methods

        /// <summary>
        /// Create spreadsheet in specified folder on your drive
        /// </summary>
        /// <param name="FileName"></param>
        /// <param name="ParentFolfer"></param>
        /// <returns>ID docu</returns>
        public string CreateSpreadsheet(string FileName, string ParentFolfer, string Description)
        {
            string Parent = ParentFolfer;

            Google.Apis.Drive.v3.Data.File fileMetadata = new Google.Apis.Drive.v3.Data.File()
            {
                Name = FileName,
                MimeType = "application/vnd.google-apps.spreadsheet",
                Description = Description,
                Parents = new List<string> { Parent }
            };

            FilesResource.CreateRequest request = driveService.Files.Create(fileMetadata);
            request.SupportsTeamDrives = true;
            request.Fields = "id";

            Google.Apis.Drive.v3.Data.File file = null;
        retry:
            try
            {
                file = request.Execute();
            }
            catch (Exception CreateSpreadsheet_Ex)
            {
                if(CreateSpreadsheet_Ex.Message.Contains("Quota exceeded for quota group 'WriteGroup' and limit 'Write requests per user per 100 seconds'"))
                {
                    Log.log(Logger.LogLevel.Warning, string.Format("Quota exceeded on ctreating spreadsheet {0}. Sleeping time : {1}", FileName, SleepTime));
                    Thread.Sleep(SleepTime);
                    goto retry;
                }
                else
                {
                    string Err = string.Format("Something wrong on ctreating spreadsheet {0}. Error : {1}", FileName, CreateSpreadsheet_Ex.Message);
                    Log.log(Logger.LogLevel.Error, Err);
                    return null;
                }
            }
             
            string IDF = file.Id;
            if (Log.logLevel >= LogLevel.Trace)
            {
                Log.log(Logger.LogLevel.Trace, string.Format("Spreadsheet \"{0}\" successfully created on folder whith ID {1}.", FileName, ParentFolfer));
            }
            return IDF;
        }

        /// <summary>
        /// Create document in specified folder on your drive
        /// </summary>
        /// <param name="FileName"></param>
        /// <param name="ParentFolfer"></param>
        /// <param name="Description"></param>
        /// <returns></returns>
        public string CreateDocument(string FileName, string ParentFolfer, string Description)
        {
            string Parent = ParentFolfer;

            Google.Apis.Drive.v3.Data.File fileMetadata = new Google.Apis.Drive.v3.Data.File()
            {
                Name = FileName,
                MimeType = "application/vnd.google-apps.document",
                Description = Description,
                Parents = new List<string> { Parent }
            };

            FilesResource.CreateRequest request = driveService.Files.Create(fileMetadata);
            request.SupportsTeamDrives = true;
            request.Fields = "id";

            Google.Apis.Drive.v3.Data.File file = null;
        retry:
            try
            {
                file = request.Execute();
            }
            catch (Exception CreateDocument_Ex)
            {
                if (CreateDocument_Ex.Message.Contains("Quota exceeded for quota group 'WriteGroup' and limit 'Write requests per user per 100 seconds'"))
                {
                    Log.log(Logger.LogLevel.Warning, string.Format("Quota exceeded on ctreating document {0}. Sleeping time : {1}", FileName, SleepTime));
                    Thread.Sleep(SleepTime);
                    goto retry;
                }
                else
                {
                    string Err = string.Format("Something wrong on ctreating document {0}. Error : {1}", FileName, CreateDocument_Ex.Message);
                    Log.log(Logger.LogLevel.Error, Err);
                    return null;
                }  
            }

            string IDF = file.Id;

            Log.log(Logger.LogLevel.Trace, string.Format("Document \"{0}\" successfully created on folder whith ID {1}.", FileName, ParentFolfer));
            return IDF;
        }

        /// <summary>
        /// Create folder in specified folder on your drive
        /// </summary>
        /// <param name="Folder"></param>
        /// <param name="ParentFolfer"></param>
        /// <param name="Description"></param>
        /// <returns></returns>
        public string CreateFolder(string Folder, string ParentFolfer, string Description)
        {
            string Parent = ParentFolfer;

            Google.Apis.Drive.v3.Data.File fileMetadata = new Google.Apis.Drive.v3.Data.File()
            {
                Name = Folder,
                MimeType = "application/vnd.google-apps.folder",
                Description = Description,
                Parents = new List<string> { Parent }
            };

            FilesResource.CreateRequest request = driveService.Files.Create(fileMetadata);
            request.SupportsTeamDrives = true;
            request.Fields = "id";

            Google.Apis.Drive.v3.Data.File file = null;
        retry:
            try
            {
                file = request.Execute();
            }
            catch (Exception CreateFolder_Ex)
            {
                if (CreateFolder_Ex.Message.Contains("Quota exceeded for quota group 'WriteGroup' and limit 'Write requests per user per 100 seconds'"))
                {
                    Log.log(Logger.LogLevel.Warning, string.Format("Quota exceeded on ctreating folder {0}. Sleeping time : {1}", Folder, SleepTime));
                    Thread.Sleep(SleepTime);
                    goto retry;
                }
                else
                {
                    string Err = string.Format("Something wrong on ctreating folder {0}. Error : {1}", Folder, CreateFolder_Ex.Message);
                    Log.log(Logger.LogLevel.Error, Err);
                    return null;
                }
            }

            string IDF = file.Id;

            Log.log(Logger.LogLevel.Trace, string.Format("Folder \"{0}\" successfully created on folder whith ID {1}.", Folder, ParentFolfer));
            return IDF;
        }

        /// <summary>
        /// Async download file from google drive
        /// </summary>
        /// <param name="documentId"></param>
        /// <param name="FolderPath"></param>
        /// <returns></returns>
        public async Task DownloadFile(string documentId, string FolderPath)
        {
            FilesResource.GetRequest fileRequest = driveService.Files.Get(documentId);
            fileRequest.Fields = "*";
            fileRequest.SupportsAllDrives = true;

            Google.Apis.Drive.v3.Data.File fileResponse;
        retry:
            try
            {
                fileResponse = fileRequest.Execute();
            }
            catch(Exception DownloadFile_ex)
            {
                if (DownloadFile_ex.Message.Contains("Quota exceeded for quota group 'WriteGroup' and limit 'Write requests per user per 100 seconds'"))
                {
                    Log.log(Logger.LogLevel.Warning, string.Format("Quota exceeded on executing GET_file_request for document with ID {0}. Sleeping time : {1}", documentId, SleepTime));
                    Thread.Sleep(SleepTime);
                    goto retry;
                }
                else
                {
                    string Err = string.Format("Something wrong on executing GET_file_request for document with ID {0}. Error : {1}", documentId, DownloadFile_ex.Message);
                    Log.log(Logger.LogLevel.Error, Err);
                    return;
                }
            }

            FilesResource.ExportRequest exportRequest = null;

            MemoryStream outputStream = new MemoryStream();

            if (fileResponse.MimeType == "application/vnd.google-apps.spreadsheet" ^
                fileResponse.MimeType == "application/vnd.google-apps.document")
            {
                exportRequest = driveService.Files.Export(documentId, "application/pdf");

                exportRequest.MediaDownloader.ProgressChanged += (IDownloadProgress progress) =>
                {
                    switch (progress.Status)
                    {
                        case DownloadStatus.Downloading:
                            {
                                Console.WriteLine(progress.BytesDownloaded);
                                break;
                            }
                        case DownloadStatus.Completed:
                            {
                                Log.log(Logger.LogLevel.Trace, string.Format("Download complete."));
                                Console.WriteLine("Download complete.");
                                using (FileStream file = new FileStream(FolderPath + fileResponse.Name + ".PDF", FileMode.OpenOrCreate, FileAccess.ReadWrite))
                                {
                                    outputStream.WriteTo(file);
                                }
                                break;
                            }
                        case DownloadStatus.Failed:
                            {
                                Log.log(Logger.LogLevel.Warning, string.Format("Download failed."));
                                Console.WriteLine("Download failed.");
                                break;
                            }
                    }

                };

                exportRequest.Download(outputStream);
                Log.log(Logger.LogLevel.Trace, string.Format("File \"{1}\" successfully downloaded to {0}.", FolderPath, fileResponse.Name));
                return;

            }
            else
            {
                Int32 KB = 0x400;
                long chunkSize = 1024 * KB; // 1Mb;

                var Request = driveService.Files.Get(documentId);
                Request.Fields = "*";
                Request.SupportsAllDrives = true;

                try
                {
                    fileResponse = fileRequest.Execute();
                }
                catch (Exception DownloadFile_ex)
                {
                    if (DownloadFile_ex.Message.Contains("Quota exceeded for quota group 'WriteGroup' and limit 'Write requests per user per 100 seconds'"))
                    {
                        Log.log(Logger.LogLevel.Warning, string.Format("Quota exceeded on executing GET_file_request for document with ID {0}. Sleeping time : {1}", documentId, SleepTime));
                        Thread.Sleep(SleepTime);
                        goto retry;
                    }
                    else
                    {
                        string Err = string.Format("Something wrong on executing GET_file_request for document with ID {0}. Error : {1}", documentId, DownloadFile_ex.Message);
                        Log.log(Logger.LogLevel.Error, Err);
                        return;
                    }
                }

                ConfigurableHttpClient client = Request.Service.HttpClient;

                long size = (long)fileResponse.Size;
                try
                {
                    using (FileStream file = new FileStream(FolderPath + fileResponse.Name, FileMode.OpenOrCreate, FileAccess.ReadWrite))
                    {
                        //Reserv memory for file
                        try
                        {
                            file.SetLength(size);
                        }
                        catch (Exception MemoryReserving_Ex)
                        {
                            string Err = String.Format("An error occurred in reserved disk space for file(size {0} Gb): {1}", size / (1024 * 1024 * 1024), MemoryReserving_Ex.Message.Split(new char[] { '\r' })[0]);
                            Log.log(Logger.LogLevel.Error, Err);
                            return;
                        }

                        long chunks = (size / chunkSize) + 1;
                        for (long index = 0; index < chunks; index++)
                        {
                            long from = index * chunkSize;
                            long to = @from + chunkSize - 1;
                            RangeHeaderValue Range = new RangeHeaderValue(from, to);

                            //VALIDATE SUCCESSFUL REQUEST
                            var request = Request.CreateRequest();
                            request.Headers.Range = Range;

                            var/*Task*/ response = await client.SendAsync(request);
                            //response.Wait();

                            //ERROR OCURRED
                            if (response.StatusCode != HttpStatusCode.PartialContent && !response.IsSuccessStatusCode)
                            {
                                string Err = response.ToString();
                                Log.log(Logger.LogLevel.Error, Err);
                                continue;
                            }
                            //IF NO ERRORS, PERFORM DOWNLOAD RANGE
                            if (response.StatusCode == HttpStatusCode.PartialContent || response.IsSuccessStatusCode)
                            {
                                
                                Console.WriteLine($"ChunkIndex: {index}; File Size: {size}; Bytes Downloaded: {from} ({Convert.ToDecimal(from) / (1024.0m * 1024.0m)}) MB; ");
                                file.Seek(@from, SeekOrigin.Begin);
                                await Request.DownloadRangeAsync(file, Range);
                                Log.log(Logger.LogLevel.Trace, string.Format("File \"{1}\" successfully downloaded to {0}.", FolderPath, fileResponse.Name));
                            }
                        }
                    }
                }
                catch (Exception FileDownloading_Ex)
                {
                    string Err = String.Format("An error occurred in downloading large file. Error : {0}", FileDownloading_Ex.Message);
                    Log.log(Logger.LogLevel.Error, Err);
                }


            }
        }

        /// <summary>
        /// Upload file to Folder with ID = driveFolderId.
        /// </summary>
        /// <param name="File"></param>
        /// <param name="driveFolderId"></param>
        /// <returns></returns>
        public Google.Apis.Drive.v3.Data.File UploadFile(string File, string driveFolderId)
        {
            
            FileInfo fileInfo = new FileInfo(File);
            string FileName = fileInfo.Name;
            string MimeType = MimeMapping.GetMimeMapping(FileName);


            Google.Apis.Drive.v3.Data.File fileMetadata = new Google.Apis.Drive.v3.Data.File()
            {
                Name = FileName,
                MimeType = MimeType,
                Parents = new List<string> { driveFolderId }
            };
            byte[] byteArray = System.IO.File.ReadAllBytes(File);

            MemoryStream MemStream = new MemoryStream(byteArray);

            FilesResource.CreateMediaUpload request = driveService.Files.Create(fileMetadata, MemStream, MimeType);
            request.Fields = "*";
            //await request.UploadAsync();

        retry:
            try
            {
                request.Upload();

                Log.log(Logger.LogLevel.Trace, string.Format("File \"{0}\" successfully downloaded to folder with ID {1}.", File, driveFolderId));
                return request.ResponseBody;
            }
            catch (Exception UploadFile_Ex)
            {
                if (UploadFile_Ex.Message.Contains("Quota exceeded for quota group 'WriteGroup' and limit 'Write requests per user per 100 seconds'"))
                {
                    Log.log(Logger.LogLevel.Warning, string.Format("Quota exceeded on uploading file {0}. Sleeping time : {1}", FileName, SleepTime));
                    Thread.Sleep(SleepTime);
                    goto retry;
                }
                else
                {
                    string Err = string.Format("Something wrong on uploading file \"{0}\" to folder with ID (1). Error : {2}", File, driveFolderId, UploadFile_Ex.Message);
                    Log.log(Logger.LogLevel.Error, Err);
                    return null;
                }
            }
        }

        /// <summary>
        /// Update existing file on google drive
        /// </summary>
        /// <param name="File"></param>
        /// <param name="driveFolderId"></param>
        /// <param name="FileId"></param>
        /// <returns></returns>
        public Google.Apis.Drive.v3.Data.File UpdateFile(string File, string driveFolderId, string FileId)
        {
            FileInfo fileInfo = new FileInfo(File);
            string FileName = fileInfo.Name;
            string MimeType = MimeMapping.GetMimeMapping(FileName);

            if (System.IO.File.Exists(File))
            {
                Google.Apis.Drive.v3.Data.File body = new Google.Apis.Drive.v3.Data.File()
                {
                    Name = FileName,
                    Description = string.Format("File updated by service {0}", driveService.ApplicationName),
                    MimeType = MimeType,
                    Parents = new List<string> { driveFolderId }
                };

                byte[] byteArray = System.IO.File.ReadAllBytes(File);

                MemoryStream stream = new MemoryStream(byteArray);

                FilesResource.UpdateMediaUpload request = driveService.Files.Update(body, FileId, stream, MimeType);
                request.Fields = "*";
            retry:
                try
                {
                    request.Upload();
                    if (Log.logLevel >= LogLevel.Trace)
                    {
                        Log.log(Logger.LogLevel.Trace, string.Format("File \"{0}\" with ID {1} successfully udated on folder with ID {2}.", File, FileId, driveFolderId));
                    }
                    return request.ResponseBody;
                }
                catch (Exception UpdateFile_Ex)
                {
                    if (UpdateFile_Ex.Message.Contains("Quota exceeded for quota group 'WriteGroup' and limit 'Write requests per user per 100 seconds'"))
                    {
                        Log.log(Logger.LogLevel.Warning, string.Format("Quota exceeded on updating file \"{0}\" with ID {1} on folder with ID {2}. Sleeping time : {3}", File, FileId, driveFolderId, SleepTime));
                        Thread.Sleep(SleepTime);
                        goto retry;
                    }
                    else
                    {
                        string Err = string.Format("Something wrong on updating file \"{0}\" with ID {1} on folder with ID {2}. Error : {3}", File, FileId, driveFolderId, UpdateFile_Ex.Message);
                        Log.log(Logger.LogLevel.Error, Err);
                        return null;
                    }
                    
                }
            }
            else
            {
                string Err = string.Format("File \"{0}\" is not exist.", File);
                Log.log(Logger.LogLevel.Error, Err);
                return null;
            }

        }

        /// <summary>
        /// Rename file on google drive
        /// </summary>
        /// <param name="FileId"></param>
        /// <param name="NewName"></param>
        /// <returns></returns>
        public Google.Apis.Drive.v3.Data.File RenameFile(string FileId, string NewName)
        {
            Google.Apis.Drive.v3.Data.File file = new Google.Apis.Drive.v3.Data.File()
            {
                Name = NewName
            };

            FilesResource.UpdateRequest request = driveService.Files.Update(file, FileId);
            request.Fields = "*";
        retry:
            try
            {
                Google.Apis.Drive.v3.Data.File renamed =  request.Execute();
                if (Log.logLevel >= LogLevel.Trace)
                {
                    Log.log(Logger.LogLevel.Trace, string.Format("File with ID {0} successfully renamed to \"{1}\".", FileId, NewName));
                }
                return renamed;
            }
            catch (Exception RenameFile_Ex)
            {
                if (RenameFile_Ex.Message.Contains("Quota exceeded for quota group 'WriteGroup' and limit 'Write requests per user per 100 seconds'"))
                {
                    Log.log(Logger.LogLevel.Warning, string.Format("Quota exceeded on renaming file with ID {0} to {1}. Sleeping time : {2}", FileId, NewName, SleepTime));
                    Thread.Sleep(SleepTime);
                    goto retry;
                }
                else
                {
                    string Err = string.Format("Something wrong on renaming file with ID {0} to {1}. Error : {2}", FileId, NewName, RenameFile_Ex.Message);
                    Log.log(Logger.LogLevel.Error, Err);
                    return null;
                }
            }
        }

        /// <summary>
        /// Delete file from google drive. It skips the trash.
        /// </summary>
        /// <param name="FileId"></param>
        public void DeleteFile(string FileId)
        {
            FilesResource.DeleteRequest DeleteRequest = driveService.Files.Delete(FileId);
        retry:
            try
            {
                DeleteRequest.Execute();
                if (Log.logLevel >= LogLevel.Trace)
                {
                    Log.log(Logger.LogLevel.Trace, string.Format("File with ID {0} successfully deleted.", FileId));
                }
            }
            catch (Exception DeleteFile_Ex)
            {
                if (DeleteFile_Ex.Message.Contains("Quota exceeded for quota group 'WriteGroup' and limit 'Write requests per user per 100 seconds'"))
                {
                    Log.log(Logger.LogLevel.Warning, string.Format("Quota exceeded on deleting file with ID {0}. Sleeping time : {1}", FileId, SleepTime));
                    Thread.Sleep(SleepTime);
                    goto retry;
                }
                else
                {
                    string Err = string.Format("Something wrong on deleting file with ID {0} . Error : {1}", FileId, DeleteFile_Ex.Message);
                    Log.log(Logger.LogLevel.Error, Err);
                }
            }
        }

        /// <summary>
        /// Delete file to trash.
        /// </summary>
        /// <param name="FileId"></param>
        public void MoveFileToTrash(string FileId)
        {
            Google.Apis.Drive.v3.Data.File file = new Google.Apis.Drive.v3.Data.File()
            {
                Trashed = true
            };
        retry:
            try
            {
                driveService.Files.Update(file, FileId).Execute();
                if (Log.logLevel >= LogLevel.Trace)
                {
                    Log.log(Logger.LogLevel.Trace, string.Format("File with ID {0} successfully moved to trash.", FileId));
                }
            }
            catch (Exception MoveFileToTrash_Ex)
            {
                if (MoveFileToTrash_Ex.Message.Contains("Quota exceeded for quota group 'WriteGroup' and limit 'Write requests per user per 100 seconds'"))
                {
                    Log.log(Logger.LogLevel.Warning, string.Format("Quota exceeded on moved to trash file with ID {0}. Sleeping time : {1}", FileId, SleepTime));
                    Thread.Sleep(SleepTime);
                    goto retry;
                }
                else
                {
                    string Err = string.Format("Something wrong on moved to trash file with ID {0} . Error : {1}", FileId, MoveFileToTrash_Ex.Message);
                    Log.log(Logger.LogLevel.Error, Err);
                }
            }
        }

        /// <summary>
        /// Delete all files from trash
        /// </summary>
        public void EmptyTrash()
        {
        retry:
            try
            {
                driveService.Files.EmptyTrash();
                if (Log.logLevel >= LogLevel.Trace)
                {
                    Log.log(Logger.LogLevel.Trace, string.Format("Trash emptied successfully."));
                }
            }
            catch (Exception EmptyTrash_Ex)
            {
                if (EmptyTrash_Ex.Message.Contains("Quota exceeded for quota group 'WriteGroup' and limit 'Write requests per user per 100 seconds'"))
                {
                    Log.log(Logger.LogLevel.Warning, string.Format("Quota exceeded on emptying trash. Sleeping time : {0}", SleepTime));
                    Thread.Sleep(SleepTime);
                    goto retry;
                }
                else
                {
                    string Err = string.Format("Something wrong on emptying trash. Error : {0}", EmptyTrash_Ex.Message);
                    Log.log(Logger.LogLevel.Error, Err);
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------------------------------------------
        //--------------------------------------------------------------------------------------------------------------------------------------------------
        //--------------------------------------------------------------------------------------------------------------------------------------------------
        // пока без логирования. надо тестировать.
        /// <summary>
        /// Get list of visible viles from google drive.
        /// </summary>
        /// <returns></returns>
        public IList<Google.Apis.Drive.v3.Data.File> GetVisibleFiles()
        {
            // Building the initial request.
            var request = driveService.Files.List();
            request.Fields = "*";
            request.PageSize = 1000;
            request.PrettyPrint = true;
            request.SupportsAllDrives = true;
            request.IncludeItemsFromAllDrives = true;
            request.SupportsTeamDrives = true;

            var pageStreamer = new Google.Apis.Requests.PageStreamer<Google.Apis.Drive.v3.Data.File, FilesResource.ListRequest, Google.Apis.Drive.v3.Data.FileList, string>(
                                                       (req, token) => request.PageToken = token,
                                                       response => response.NextPageToken,
                                                       response => response.Files);

            FileList AllFiles = new Google.Apis.Drive.v3.Data.FileList();
            AllFiles.Files = new List<Google.Apis.Drive.v3.Data.File>();



            foreach (var result in pageStreamer.Fetch(request))
            {
                AllFiles.Files.Add(result);
            }

            return AllFiles.Files;
        }

        /// <summary>
        /// Print list of visible files
        /// </summary>
        /// <param name="List"></param>
        /// <param name="indent"></param>
        public void PrettyPrint(IList<Google.Apis.Drive.v3.Data.File> List, string indent)
        {
            foreach (var item in List.OrderBy(a => a.CreatedTimeRaw))
            {
                Console.WriteLine(string.Format("{0}|-{1}.{2}", indent, item.Name, item.FileExtension));

                if (item.MimeType == "application/vnd.google-apps.folder")
                {
                    Google.Apis.Drive.v3.Data.File T = new Google.Apis.Drive.v3.Data.File();
                    T.Parents = new List<string> { item.Id };
                    var NotNull = List.Where(x => x.Parents != null).ToList();
                    var ChildrenFiles = NotNull.Where(x => x.Parents.ToList().SequenceEqual(T.Parents)).ToList();
                    PrettyPrint(ChildrenFiles, indent + "______");
                }
            }
        }

        //--------------------------------------------------------------------------------------------------------------------------------------------------
        //--------------------------------------------------------------------------------------------------------------------------------------------------
        //--------------------------------------------------------------------------------------------------------------------------------------------------

        /// <summary>
        /// Create new permisiion for file or directory
        /// </summary>
        /// <param name="documentId"></param>
        /// <param name="type"></param>
        /// <param name="email"></param>
        /// <param name="role"></param>
        public void CreateNewPermission(string documentId, string type, string email, string role)
        {
            Permission perm = new Permission();
            perm.Type = type;// "user"
            perm.EmailAddress = email;
            perm.Role = role;//"owner"

            PermissionsResource.CreateRequest req = driveService.Permissions.Create(perm, documentId);
            req.TransferOwnership = true;
        retry:
            try
            {
                req.Execute();
                if (Log.logLevel >= LogLevel.Trace)
                {
                    Log.log(Logger.LogLevel.Trace, string.Format("New permission for {0} \"{1}\" with role {2} for abstract {3} successfully created.", type, email, role, documentId));
                }
            }
            catch (Exception CreateNewPermission_Ex)
            {
                if (CreateNewPermission_Ex.Message.Contains("Quota exceeded for quota group 'WriteGroup' and limit 'Write requests per user per 100 seconds'"))
                {
                    Log.log(Logger.LogLevel.Warning, string.Format("Quota exceeded on creating new permission for {0} \"{1}\" with role {2} for abstract {3}. Sleeping time : {4}", type, email, role, documentId, SleepTime));
                    Thread.Sleep(SleepTime);
                    goto retry;
                }
                else
                {
                    string Err = string.Format("Something wrong on creating new permission for {0} \"{1}\" with role {2} for abstract {3} . Error : {4}", type, email, role, documentId, CreateNewPermission_Ex.Message);
                    Log.log(Logger.LogLevel.Error, Err);
                }
            }
        }

        /// <summary>
        /// Update permission by permission Id for file or directory
        /// </summary>
        /// <param name="documentId"></param>
        /// <param name="permissionId"></param>
        /// <param name="newRole"></param>
        public void UpdatePermission(string documentId, string email, string newRole)
        {
            string permissionId = "";
            IList<Permission> permissions = GetPermissionsFromFile(documentId);
            foreach (Permission P in permissions)
            {
                if (P.EmailAddress == email)
                {
                    permissionId = P.Id;
                }
            }

            Permission permission = new Permission
            {
                Role = newRole
            };
            var req = driveService.Permissions.Update(permission, documentId, permissionId);
            if (newRole == "owner")
            {
                req.TransferOwnership = true;
                req.SupportsAllDrives = true;
            }
        retry:
            try
            {
                req.Execute();
                if (Log.logLevel >= LogLevel.Trace)
                {
                    Log.log(Logger.LogLevel.Trace, string.Format("New permission for \"{0}\" on abstract {1} successfully updated to {2}.", email, documentId, newRole));
                }
            }
            catch (Exception UpdatePermission_Ex)
            {
                if (UpdatePermission_Ex.Message.Contains("Quota exceeded for quota group 'WriteGroup' and limit 'Write requests per user per 100 seconds'"))
                {
                    Log.log(Logger.LogLevel.Warning, string.Format("Quota exceeded on updating permission to {0} for \"{1}\" on abstract {2} . Sleeping time : {3}", newRole, email, documentId, SleepTime));
                    Thread.Sleep(SleepTime);
                    goto retry;
                }
                else
                {
                    string Err = string.Format("Something wrong on updating permission to {0} for \"{1}\" on abstract {2} . Error : {3}", newRole, email, documentId, UpdatePermission_Ex.Message);
                    Log.log(Logger.LogLevel.Error, Err);
                }
            }
        }

        /// <summary>
        /// View permissions parametrs for file or directory
        /// </summary>
        /// <param name="documentId"></param>
        /// <returns></returns>
        public IList<Permission> GetPermissionsFromFile(string documentId)
        {
            PermissionsResource.ListRequest request = driveService.Permissions.List(documentId);
            request.Fields = "permissions/*";
        retry:
            try
            {
                IList<Permission> PermissionsList = request.Execute().Permissions;
                if (Log.logLevel >= LogLevel.Trace)
                {
                    Log.log(Logger.LogLevel.Trace, string.Format("Extracting permissions from abstract with ID {0} successfully competed.", documentId));
                }
                return PermissionsList;
            }
            catch (Exception GetPermissionsFromFile_Ex)
            {
                if (GetPermissionsFromFile_Ex.Message.Contains("Quota exceeded for quota group 'WriteGroup' and limit 'Write requests per user per 100 seconds'"))
                {
                    Log.log(Logger.LogLevel.Warning, string.Format("Quota exceeded on extracting permissions from abstract with ID {0}. Sleeping time : {1}", documentId, SleepTime));
                    Thread.Sleep(SleepTime);
                    goto retry;
                }
                else
                {
                    string Err = string.Format("Something wrong on extracting permissions from abstract with ID {0} . Error : {1}", documentId, GetPermissionsFromFile_Ex.Message);
                    Log.log(Logger.LogLevel.Error, Err);
                }
                return null;
            }
        }

        /// <summary>
        /// Delete permission by permission Id for file or directory
        /// </summary>
        /// <param name="documentId"></param>
        /// <param name="permissionId"></param>
        public void DeletePermission(string documentId, string permissionId)
        {
        retry:
            try
            {
                driveService.Permissions.Delete(documentId, permissionId).Execute();
                if (Log.logLevel >= LogLevel.Trace)
                {
                    Log.log(Logger.LogLevel.Trace, string.Format("Deleting permission with ID {0} from abstract with ID {1} successfully competed.", permissionId, documentId));
                }
            }
            catch (Exception DeletePermission_Ex)
            {
                if (DeletePermission_Ex.Message.Contains("Quota exceeded for quota group 'WriteGroup' and limit 'Write requests per user per 100 seconds'"))
                {
                    Log.log(Logger.LogLevel.Warning, string.Format("Quota exceeded on deleting permission whith ID {0} from abstract with ID {1}. Sleeping time : {2}", permissionId, documentId, SleepTime));
                    Thread.Sleep(SleepTime);
                    goto retry;
                }
                else
                {
                    string Err = string.Format("Something wrong on deleting permission whith ID {0} from abstract with ID {1} . Error : {2}", permissionId, documentId, DeletePermission_Ex.Message);
                    Log.log(Logger.LogLevel.Error, Err);
                }
            }
        }

        #endregion
    }
}
