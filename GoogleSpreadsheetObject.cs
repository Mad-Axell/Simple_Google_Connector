using Google.Apis.Sheets.v4;
using Google.Apis.Sheets.v4.Data;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Services;
using System.Collections.Generic;

using System.Threading;
using System;
using System.Linq;

using Logger;
using Newtonsoft.Json;

namespace GoogleConnector
{
    public class GoogleSpreadsheetObject
    {
        #region Fields
        private Int32 _sleeptime;
        private Int32 _retrys;
        private string _applicationname;
        private string[] _scopes;
        private GoogleCredential _credential;
        private SheetsService _service;
        #endregion

        #region Props
        public Int32 SleepTime
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
        public Int32 Retrys
        {
            set
            {
                this._retrys = value;
                if (Log.logLevel >= LogLevel.Trace)
                {
                    Log.log(Logger.LogLevel.Trace, string.Format("For Int32 Retrys set value {0}", value));
                }
            }
            get
            {
                return this._retrys;
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
                    Log.log(Logger.LogLevel.Trace, string.Format("For string[] Scopes set value {0}", JsonConvert.SerializeObject(_scopes)));
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
                    Log.log(Logger.LogLevel.Trace, string.Format("For GoogleCredential credential set value {0}", value.ToString()));
                }
            }
            get
            {
                return this._credential;
            }
        }
        public SheetsService service
        {
            set
            {
                this._service = value;
                if (Log.logLevel >= LogLevel.Trace)
                {
                    Log.log(Logger.LogLevel.Trace, string.Format("For SheetsService service set value {0}", value.ToString()));
                }
            }
            get
            {
                return this._service;
            }
        }
        #endregion

        #region Constructor
        public GoogleSpreadsheetObject(string Credential, string applicationname, Int32 sleep, Int32 retrys)
        {
            SleepTime = sleep;
            Retrys = retrys;
            ApplicationName = applicationname;
            Scopes = new string[] { SheetsService.Scope.Spreadsheets };
            credential = GoogleCredential.FromJson(Credential).CreateScoped(Scopes);

            service = new SheetsService(new BaseClientService.Initializer()
            {
                HttpClientInitializer = credential,
                ApplicationName = ApplicationName,
            });

            if (Log.logLevel >= LogLevel.Trace)
            {
                Log.log(Logger.LogLevel.Trace, string.Format("GoogleSpreadsheetObject was successfully created"));
            }
        }
        #endregion

        #region Methods

        public bool BatchUpdateRequestExecute (BatchUpdateSpreadsheetRequest Request, string SpreadsheetId)
        {
            SpreadsheetsResource.BatchUpdateRequest Batch_Update_Request =
                service.Spreadsheets.BatchUpdate(Request, SpreadsheetId);

            int retry_count = 0;

        retry:
            try
            {
                Batch_Update_Request.Execute();
                retry_count++;
                return true;
            }
            catch (Exception ChangeSpreadsheetTitle_EX)
            {
                if (ChangeSpreadsheetTitle_EX.Message.Contains("Quota exceeded for quota"))
                {
                    if(retry_count <= Retrys)
                    {
                        Log.log(Logger.LogLevel.Warning, string.Format("Quota exceeded for quota group 'WriteGroup' and limit 'Write requests per user per 100 seconds'. Sleeping time : {0}", SleepTime));
                        Thread.Sleep(SleepTime);
                        goto retry;
                    }
                    else
                    {
                        Log.log(Logger.LogLevel.Warning, string.Format("Limit retrys exceeded"));
                        return false;
                    }
                }
                else
                {
                    string Err = string.Format(ChangeSpreadsheetTitle_EX.Message.ToString());
                    Log.log(Logger.LogLevel.Error, Err);
                    return false;
                }
            }
        }

        /// <summary>
        /// Меняет наименование таблицы.
        /// </summary>
        /// <param name="title">string наименование таблицы.</param>
        public void ChangeSpreadsheetTitle(string SpreadsheetId, string Title)
        {
            UpdateSpreadsheetPropertiesRequest USPR = new UpdateSpreadsheetPropertiesRequest();
            USPR.Properties = new SpreadsheetProperties();
            USPR.Properties.Title = Title;
            USPR.Fields = "title";

            BatchUpdateSpreadsheetRequest Batch_Update_Spreadsheet_Request = new BatchUpdateSpreadsheetRequest();
            Batch_Update_Spreadsheet_Request.Requests = new List<Request>() { new Request { UpdateSpreadsheetProperties = USPR }};

            bool Executor = BatchUpdateRequestExecute(Batch_Update_Spreadsheet_Request, SpreadsheetId);
            if (Executor)
            {
                if (Log.logLevel >= LogLevel.Trace)
                {
                    Log.log(Logger.LogLevel.Trace, string.Format("Spreadsheet title successfully changed on {0}.", Title));
                }
            }
            else
            {
                if (Log.logLevel >= LogLevel.Error)
                {
                    Log.log(Logger.LogLevel.Error, string.Format("Spreadsheet title not successfully changed on {0}.", Title));
                }
            }
            
        }

        /// <summary>
        /// Создаёт новый лист в таблице.
        /// </summary>
        /// <param name="Title">название листа.</param>
        public void CreateNewSheet(string SpreadsheetId, string Title)
        {
            string sheetName = Title;
            AddSheetRequest Add_Sheet_Request = new AddSheetRequest();
            Add_Sheet_Request.Properties = new SheetProperties();
            Add_Sheet_Request.Properties.Title = sheetName;
            BatchUpdateSpreadsheetRequest Batch_Update_Spreadsheet_Request = new BatchUpdateSpreadsheetRequest();
            Batch_Update_Spreadsheet_Request.Requests = new List<Request>();
            Batch_Update_Spreadsheet_Request.Requests.Add(new Request
            {
                AddSheet = Add_Sheet_Request
            });

            bool Executor = BatchUpdateRequestExecute(Batch_Update_Spreadsheet_Request, SpreadsheetId);
            if (Executor)
            {
                if (Log.logLevel >= LogLevel.Trace)
                {
                    Log.log(Logger.LogLevel.Trace, string.Format("Nwe sheet {0} successfully created on spreadsheet with ID {1}.", Title, SpreadsheetId));
                }
            }
            else
            {
                if (Log.logLevel >= LogLevel.Error)
                {
                    Log.log(Logger.LogLevel.Error, string.Format("Nwe sheet {0} not successfully created on spreadsheet with ID {1}.", Title, SpreadsheetId));
                }
            }
        }

        /// <summary>
        /// Удаляет лист из таблицы по его идентификатору.
        /// </summary>
        /// <param name="sheetId">ID листа.</param>
        public void DeleteSheet(string SpreadsheetId, int sheetId)
        {
            DeleteSheetRequest Dell_Sheet_Request = new DeleteSheetRequest();
            Dell_Sheet_Request.SheetId = sheetId;
            BatchUpdateSpreadsheetRequest Batch_Update_Spreadsheet_Request = new BatchUpdateSpreadsheetRequest();
            Batch_Update_Spreadsheet_Request.Requests = new List<Request>() { new Request { DeleteSheet = Dell_Sheet_Request } };

            bool Executor = BatchUpdateRequestExecute(Batch_Update_Spreadsheet_Request, SpreadsheetId);
            if (Executor)
            {
                if (Log.logLevel >= LogLevel.Trace)
                {
                    Log.log(Logger.LogLevel.Trace, string.Format("Sheet with id {0} successfully deleted on spreadsheet with ID {1}.", sheetId, SpreadsheetId));
                }
            }
            else
            {
                if (Log.logLevel >= LogLevel.Error)
                {
                    Log.log(Logger.LogLevel.Error, string.Format("Sheet with id {0} was not successfully deleted on spreadsheet with ID {1}.", sheetId, SpreadsheetId));
                }
            }  
        }

        public void AutoResizeColumnsWidth(string SpreadsheetId, string SheetTitle)
        {
            List<Sheet> Lists = GetLists(SpreadsheetId);


            int sheetId = Lists.Where(t => t.title == SheetTitle).ToList()[0].index;

            BatchUpdateSpreadsheetRequest Batch_Update_Spreadsheet_Request = new BatchUpdateSpreadsheetRequest();
            Batch_Update_Spreadsheet_Request.Requests = new List<Request>
            {
                new Request
                {
                    AutoResizeDimensions = new AutoResizeDimensionsRequest()
                    {
                        Dimensions = new DimensionRange()
                        {
                            SheetId = Convert.ToInt32(sheetId),
                            Dimension = "COLUMNS",
                            StartIndex = 0,
                            EndIndex = 100
                        }
                    }
                }
            };

            bool Executor = BatchUpdateRequestExecute(Batch_Update_Spreadsheet_Request, SpreadsheetId);
            if (Executor)
            {
                if (Log.logLevel >= LogLevel.Trace)
                {
                    Log.log(Logger.LogLevel.Trace, string.Format("Autoresize columns width on sheet \"{0}\" successfully сompleted on spreadsheet with ID {1}.", SheetTitle, SpreadsheetId));
                }
            }
            else
            {
                if (Log.logLevel >= LogLevel.Error)
                {
                    Log.log(Logger.LogLevel.Error, string.Format("Autoresize columns width on sheet \"{0}\" was not successfully сompleted on spreadsheet with ID {1}.", SheetTitle, SpreadsheetId));
                }
            }
        }

        public void AutoResizeColumnsWidth(string SpreadsheetId, int sheetId)
        {
            BatchUpdateSpreadsheetRequest Batch_Update_Spreadsheet_Request = new BatchUpdateSpreadsheetRequest();
            Batch_Update_Spreadsheet_Request.Requests = new List<Request>
            {
                new Request
                {
                    AutoResizeDimensions = new AutoResizeDimensionsRequest()
                    {
                        Dimensions = new DimensionRange()
                        {
                            SheetId = sheetId,
                            Dimension = "COLUMNS",
                            StartIndex = 0,
                            EndIndex = 100
                        }
                    }
                }
            };

            bool Executor = BatchUpdateRequestExecute(Batch_Update_Spreadsheet_Request, SpreadsheetId);
            if (Executor)
            {
                if (Log.logLevel >= LogLevel.Trace)
                {
                    Log.log(Logger.LogLevel.Trace, string.Format("Autoresize columns width on sheet with sheetId \"{0}\" successfully сompleted on spreadsheet with ID {1}.", sheetId, SpreadsheetId));
                }
            }
            else
            {
                if (Log.logLevel >= LogLevel.Error)
                {
                    Log.log(Logger.LogLevel.Error, string.Format("Autoresize columns width on sheet with sheetId \"{0}\" was not successfully сompleted on spreadsheet with ID {1}.", sheetId, SpreadsheetId));
                }
            }
        }

        /// <summary>
        /// Возвращает список листов внутри документа.
        /// </summary>
        /// <returns>List<GoogleSheet>Список листов в таблице.</returns>
        public List<Sheet> GetLists(string SpreadsheetId)
        {
        retry:
            Spreadsheet GetLists;
            try
            {
                GetLists = service.Spreadsheets.Get(SpreadsheetId).Execute();
            }
            catch (Exception GetLists_Ex)
            {
                if (GetLists_Ex.Message.Contains("Quota exceeded for quota"))
                {
                    Log.log(Logger.LogLevel.Warning, string.Format("Quota exceeded on getting sheets list from spreadsheet with ID {0}. Sleeping time : {1}", SpreadsheetId, SleepTime));
                    Thread.Sleep(SleepTime);
                    goto retry;
                }
                else
                {
                    string Err = string.Format("Something wrong on getting sheets list from spreadsheet with ID {1}. Error : {0}", GetLists_Ex.Message, SpreadsheetId);
                    Log.log(Logger.LogLevel.Error, Err);
                    return null;
                }
            }

            List<Sheet> Lists = new List<Sheet>();
            foreach (Google.Apis.Sheets.v4.Data.Sheet Sheet in GetLists.Sheets)
            {
                Sheet GS = new Sheet();
                GS.title = Sheet.Properties.Title.ToString();
                GS.index = Convert.ToInt32(Sheet.Properties.SheetId);

                if (!Lists.Any(x => x.title == GS.title && x.index == GS.index))
                {
                    Lists.Add(GS);
                    if (Log.logLevel >= LogLevel.Trace)
                    {
                        Log.log(Logger.LogLevel.Trace, string.Format("Sheet {0} with ID {1} added to sheets list", GS.title, GS.index));
                    }
                }
            }

            if (Log.logLevel >= LogLevel.Trace)
            {
                Log.log(Logger.LogLevel.Trace, string.Format("List<Sheet> GetLists return sheets list {0} from spreadsheet with ID {1}.", JsonConvert.SerializeObject(Lists), SpreadsheetId));
            }
            return Lists;
        }

        /// <summary>
        /// Get range from first sheet
        /// </summary>
        /// <returns></returns>
        public string GetRange(string SpreadsheetId)
        {
            // Define request parameters
            String range = "A:Z";

            SpreadsheetsResource.ValuesResource.GetRequest getRequest =
                       service.Spreadsheets.Values.Get(SpreadsheetId, range);

            ValueRange getResponse = null;
        retry:
            try
            {
                getResponse = getRequest.Execute();
            }
            catch (Exception GetRange_Ex)
            {
                if (GetRange_Ex.Message.Contains("Quota exceeded for quota"))
                {
                    Log.log(Logger.LogLevel.Warning, string.Format("Quota exceeded on getting values(GetRange) on first sheet from spreadsheet with ID {0}. Sleeping time : {1}", SpreadsheetId, SleepTime));
                    Thread.Sleep(SleepTime);
                    goto retry;
                }
                else
                {
                    string Err = string.Format("Something wrong on getting values(GetRange) on first sheet from spreadsheet with ID {0}. Error : {1}", SpreadsheetId, GetRange_Ex.Message);
                    Log.log(Logger.LogLevel.Error, Err);
                }
            }

            IList<IList<Object>> getValues = getResponse.Values;
            if (getValues == null)
            {
                // spreadsheet is empty return Row A Column A 
                string sheet_range = "A:A";
                if (Log.logLevel >= LogLevel.Trace)
                {
                    Log.log(Logger.LogLevel.Trace, string.Format("Spreadsheet is empty. String GetRange return range{0} from first sheet from spreadsheet with ID {1}.", sheet_range, SpreadsheetId));
                }
                return sheet_range;
            }

            int currentCount = getValues.Count();
            int ColumnCount = 0;
            foreach (IList<Object> str in getValues)
            {
                int CurentColumns = str.Count();
                if (currentCount > ColumnCount)
                {
                    ColumnCount = CurentColumns;
                }
            }

            string EndRange = ":A";
            string ABC = @"ABCDEFGYIJKLMNOPQRSTUVWXYZ";
            if (ColumnCount <= 26)
            {
                char symbol = ABC[ColumnCount + 1];
                EndRange = $":{symbol}";
            }
            else
            {
                int repeater = ColumnCount / 26;
                int Column = ColumnCount % 26;
                EndRange = ":";
                for (int i = 0; i < repeater; i++)
                {
                    EndRange += "Z";
                }
                char symbol = ABC[Column - 1];
                EndRange += $"{symbol}";
            }

            String newRange = "A" + currentCount + EndRange;
            if (Log.logLevel >= LogLevel.Trace)
            {
                Log.log(Logger.LogLevel.Trace, string.Format("String GetRange return range{0} from first sheet from spreadsheet with ID {1}.", newRange, SpreadsheetId));
            }
            return newRange;
        }

        /// <summary>
        /// Get curent sheet range
        /// </summary>
        /// <returns></returns>
        public string GetRange(string SpreadsheetId, string Title)
        {
            // Define request parameters
            String range;
            if (Title != "")
            {
                range = Title + "!A:Z";
            }
            else
            {
                range = "A:Z";
            }

            SpreadsheetsResource.ValuesResource.GetRequest getRequest =
                       service.Spreadsheets.Values.Get(SpreadsheetId, range);

            ValueRange getResponse = null;
        retry:
            try
            {
                getResponse = getRequest.Execute();
            }
            catch (Exception GetRange_Ex)
            {
                if (GetRange_Ex.Message.Contains("Quota exceeded for quota"))
                {
                    Log.log(Logger.LogLevel.Warning, string.Format("Quota exceeded on getting values(GetRange) on sheet \"{0}\" from spreadsheet with ID {1}. Sleeping time : {1}", Title, SpreadsheetId, SleepTime));
                    Thread.Sleep(SleepTime);
                    goto retry;
                }
                else
                {
                    string Err = string.Format("Something wrong on getting values(GetRange) on sheet \"{0}\" from spreadsheet with ID {1}. Error : {2}", Title, SpreadsheetId, GetRange_Ex.Message);
                    Log.log(Logger.LogLevel.Error, Err);
                }
            }
            
            IList<IList<Object>> getValues = getResponse.Values;
            if (getValues == null)
            {
                // spreadsheet is empty return Row A Column A  
                string sheet_range = "A:A";
                if (Log.logLevel >= LogLevel.Trace)
                {
                    Log.log(Logger.LogLevel.Trace, string.Format("Spreadsheet is empty. String GetRange return range{0} from first sheet from spreadsheet with ID {1}.", sheet_range, SpreadsheetId));
                }
                return sheet_range;
            }

            int currentCount = getValues.Count();
            int ColumnCount = 0;
            foreach (IList<Object> str in getValues)
            {
                int CurentColumns = str.Count();
                if (currentCount > ColumnCount)
                {
                    ColumnCount = CurentColumns;
                }
            }

            string EndRange = ":A";
            string ABC = @"ABCDEFGYIJKLMNOPQRSTUVWXYZ";
            if (ColumnCount <= 26)
            {
                char symbol = ABC[ColumnCount - 1];
                EndRange = $":{symbol}";
            }
            else
            {
                int repeater = ColumnCount / 26;
                int Column = ColumnCount % 26;
                EndRange = ":";
                for (int i = 0; i < repeater; i++)
                {
                    EndRange += "Z";
                }
                char symbol = ABC[Column - 1];
                EndRange += $"{symbol}";
            }



            String newRange = "A" + currentCount + EndRange;
            if (Log.logLevel >= LogLevel.Trace)
            {
                Log.log(Logger.LogLevel.Trace, string.Format("String GetRange return range {0} from sheet \"{1}\" from spreadsheet with ID {2}.", newRange, Title, SpreadsheetId));
            }
            return newRange;
        }

        /// <summary>
        /// Batch append data to curent google sheet [range = "SheetTitle!An:M"]
        /// </summary>
        /// <param name="Values"></param>
        /// <param name="newRange">range = "SheetTitle!An:M"</param>
        public void BatchAppendData(string SpreadsheetId, string newRange, IList<IList<Object>> Values)
        {
            SpreadsheetsResource.ValuesResource.AppendRequest request =
                        service.Spreadsheets.Values.Append(new ValueRange() { Values = Values }, SpreadsheetId, newRange);
            request.InsertDataOption = SpreadsheetsResource.ValuesResource.AppendRequest.InsertDataOptionEnum.INSERTROWS;
            request.ValueInputOption = SpreadsheetsResource.ValuesResource.AppendRequest.ValueInputOptionEnum.USERENTERED;//RAW
        retry:
            try
            {
                request.Execute();
            }
            catch (Exception BatchAppendData_Ex)
            {
                if (BatchAppendData_Ex.Message.Contains("Quota exceeded for quota"))
                {
                    Log.log(Logger.LogLevel.Warning, string.Format("Quota exceeded on batch appending values on sheet \"{0}\" from spreadsheet with ID {1}. Sleeping time : {2}", newRange.Split('!')[0], SpreadsheetId, SleepTime));
                    Thread.Sleep(SleepTime);
                    goto retry;
                }
                else
                {
                    string Err = string.Format("Something wrong on batch appending values on sheet \"{0}\" from spreadsheet with ID {1}. Error : {2}", newRange.Split('!')[0], SpreadsheetId, BatchAppendData_Ex.Message);
                    Log.log(Logger.LogLevel.Error, Err);
                    return;
                }
            }

            string Title = newRange.Split('!')[0];
            if (Log.logLevel >= LogLevel.Trace)
            {
                Log.log(Logger.LogLevel.Trace, string.Format("Batch appending values on sheet \"{0}\" successfully сompleted on spreadsheet with ID {1} .", newRange.Split('!')[0], SpreadsheetId));
            }
        }

        /// <summary>
        /// Batch update specified google sheet [range = "SheetTitle!An:M"] and resize columns
        /// </summary>
        /// <param name="Values"></param>
        /// <param name="newRange">range = "SheetTitle!An:M"</param>
        public void BatchUpdate(string SpreadsheetId, string newRange, IList<IList<object>> Values)
        {
            ValueRange valueRange = new ValueRange();
            valueRange.MajorDimension = "ROWS"; //"ROWS";//"COLUMNS";
            valueRange.Values = Values;

            SpreadsheetsResource.ValuesResource.UpdateRequest request =
                        service.Spreadsheets.Values.Update(valueRange, SpreadsheetId, newRange);
            //request.InsertDataOption = SpreadsheetsResource.ValuesResource.AppendRequest.InsertDataOptionEnum.INSERTROWS;
            request.ValueInputOption = SpreadsheetsResource.ValuesResource.UpdateRequest.ValueInputOptionEnum.USERENTERED;//RAW
        retry:
            try
            {
                request.Execute();
            }
            catch (Exception BatchUpdate_Ex)
            {
                if (BatchUpdate_Ex.Message.Contains("Quota exceeded for quota"))
                {
                    Log.log(Logger.LogLevel.Warning, string.Format("Quota exceeded on batch updating values on sheet \"{0}\" from spreadsheet with ID {1}. Sleeping time : {2}", newRange.Split('!')[0], SpreadsheetId, SleepTime));
                    Thread.Sleep(SleepTime);
                    goto retry;
                }
                else
                {
                    string Err = string.Format("Something wrong on batch updating values on sheet \"{0}\" from spreadsheet with ID {1}. Error : {2}", newRange.Split('!')[0], SpreadsheetId, BatchUpdate_Ex.Message);
                    Log.log(Logger.LogLevel.Error, Err);
                    return;
                }
            }

            string Title = newRange.Split('!')[0];
            
            if (Log.logLevel >= LogLevel.Trace)
            {
                Log.log(Logger.LogLevel.Trace, string.Format("Batch updating values on sheet \"{0}\" successfully сompleted on spreadsheet with ID {1} .", newRange.Split('!')[0], SpreadsheetId));
            }
        }

        /// <summary>
        /// Get data from spresdsheet
        /// </summary>
        /// <param name="SpreadSheetId"></param>
        /// <returns></returns>
        public IList<IList<object>> GetData(string SpreadsheetId, string Range)
        {
            SpreadsheetsResource.ValuesResource.GetRequest request =
                    service.Spreadsheets.Values.Get(SpreadsheetId, Range);

        retry:
            ValueRange MT_response = new ValueRange();
            try
            {
                MT_response = request.Execute();
            }
            catch (Exception GetData_Ex)
            {
                if (GetData_Ex.Message.Contains("Quota exceeded for quota"))
                {
                    Log.log(Logger.LogLevel.Warning, string.Format("Quota exceeded on getting values from sheet \"{0}\" on spreadsheet with ID {1}. Sleeping time : {2}", Range.Split('!')[0], SpreadsheetId, SleepTime));
                    Thread.Sleep(SleepTime);
                    goto retry;
                }
                else
                {
                    string Err = string.Format("Something wrong on getting values from sheet \"{0}\" on spreadsheet with ID {1}. Error : {2}", Range.Split('!')[0], SpreadsheetId, GetData_Ex.Message);
                    Log.log(Logger.LogLevel.Error, Err);
                }
            }

            IList<IList<Object>> Data = MT_response.Values;

            if (Data != null && Data.Count > 0)
            {
                if (Log.logLevel >= LogLevel.Trace)
                {
                    Log.log(Logger.LogLevel.Trace, string.Format("Getting values on sheet \"{0}\" successfully сompleted on spreadsheet with ID {1} .", Range.Split('!')[0], SpreadsheetId));
                }
                return Data;
            }
            else
            {
                string Err = string.Format("Sheet \"{0}\" on spreadsheet with ID {1} is empty.", Range.Split('!')[0], SpreadsheetId);
                Log.log(Logger.LogLevel.Warning, Err);
                return null;
            }
        }
        #endregion
    }

    public class Sheet
    {
        public string title;
        public int index;
    }
}
