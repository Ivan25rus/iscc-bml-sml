#property strict
#property version "1.00"

#define MAX_BATCHES 8
#define MAX_POSITIONS 100

input long ExpectedAccount=0;
input string ExpectedServer="";

struct CloseTicket
{
   MqlTradeRequest request;
   bool submitted;
   bool finished;
   uint request_id;
   uint retcode;
   ulong order;
   double volume;
   double price;
   double dispatch_ms;
};

struct CloseBatch
{
   string id;
   long expires_ms;
   ulong started;
   int count;
   CloseTicket tickets[MAX_POSITIONS];
};

CloseBatch batches[MAX_BATCHES];
string directory;
ulong last_heartbeat;
bool ready=false;

bool AccountMatches()
{
   return AccountInfoInteger(ACCOUNT_LOGIN)==ExpectedAccount && AccountInfoString(ACCOUNT_SERVER)==ExpectedServer;
}

bool CanClose()
{
   return AccountMatches() && TerminalInfoInteger(TERMINAL_CONNECTED) && TerminalInfoInteger(TERMINAL_TRADE_ALLOWED)
          && MQLInfoInteger(MQL_TRADE_ALLOWED) && AccountInfoInteger(ACCOUNT_TRADE_ALLOWED)
          && AccountInfoInteger(ACCOUNT_TRADE_EXPERT)
          && AccountInfoInteger(ACCOUNT_MARGIN_MODE)==ACCOUNT_MARGIN_MODE_RETAIL_HEDGING;
}

bool Current(const long expires_ms)
{
   const long now_ms=(long)TimeGMT()*1000;
   // TimeGMT has second precision. Require the entire current second to be
   // inside the deadline, including its last millisecond.
   return expires_ms>now_ms+999 && expires_ms<=now_ms+4000;
}

bool SaveReady()
{
   const string pending=directory+"ready.tmp";
   const int file=FileOpen(pending,FILE_WRITE|FILE_TXT|FILE_ANSI,0,CP_UTF8);
   if(file==INVALID_HANDLE) return false;
   const bool wrote=FileWriteString(file,"1\t"+(string)ExpectedAccount+"\t"+ExpectedServer+"\n")>0;
   FileClose(file);
   return wrote && FileMove(pending,0,directory+"ready",FILE_REWRITE);
}

bool ValidId(const string id)
{
   if(StringLen(id)!=64) return false;
   for(int i=0;i<64;i++)
   {
      const ushort c=StringGetCharacter(id,i);
      if(!((c>='0' && c<='9') || (c>='a' && c<='f'))) return false;
   }
   return true;
}

bool LoadBatch(const string claimed,const string id,CloseBatch &batch)
{
   const int file=FileOpen(claimed,FILE_READ|FILE_TXT|FILE_ANSI,0,CP_UTF8);
   if(file==INVALID_HANDLE) return false;
   string fields[];
   if(FileSize(file)>16384 || StringSplit(FileReadString(file),'\t',fields)!=6
      || fields[0]!="1" || fields[1]!=id || fields[2]!=(string)ExpectedAccount || fields[3]!=ExpectedServer)
   {
      FileClose(file);
      return false;
   }
   const int count=(int)StringToInteger(fields[5]);
   batch.expires_ms=StringToInteger(fields[4]);
   if(count<2 || count>MAX_POSITIONS)
   {
      FileClose(file);
      return false;
   }
   for(int i=0;i<count;i++)
   {
      if(StringSplit(FileReadString(file),'\t',fields)!=7)
      {
         FileClose(file);
         return false;
      }
      ZeroMemory(batch.tickets[i]);
      const string ticket=fields[0];
      const ulong position=(ulong)StringToInteger(ticket);
      const string symbol=fields[1];
      const double volume=StringToDouble(fields[2]);
      const long side=StringToInteger(fields[3]);
      const long filling=StringToInteger(fields[4]);
      const long deviation=StringToInteger(fields[5]);
      const long magic=StringToInteger(fields[6]);
      if(position==0 || (string)position!=ticket || symbol=="" || !MathIsValidNumber(volume) || volume<=0
         || (side!=ORDER_TYPE_BUY && side!=ORDER_TYPE_SELL) || filling<ORDER_FILLING_FOK
         || filling>ORDER_FILLING_RETURN || deviation<0 || magic<0)
      {
         FileClose(file);
         return false;
      }
      for(int j=0;j<i;j++)
         if(batch.tickets[j].request.position==position || batch.tickets[j].request.symbol!=symbol)
         {
            FileClose(file);
            return false;
         }
      batch.tickets[i].request.action=TRADE_ACTION_DEAL;
      batch.tickets[i].request.position=position;
      batch.tickets[i].request.symbol=symbol;
      batch.tickets[i].request.volume=volume;
      batch.tickets[i].request.type=(ENUM_ORDER_TYPE)side;
      batch.tickets[i].request.type_filling=(ENUM_ORDER_TYPE_FILLING)filling;
      batch.tickets[i].request.deviation=(ulong)deviation;
      batch.tickets[i].request.magic=(ulong)magic;
      batch.tickets[i].request.comment="ISCC close";
   }
   bool complete=FileIsEnding(file);
   FileClose(file);
   if(!complete) return false;
   batch.id=id;
   batch.count=count;
   batch.started=GetMicrosecondCount();
   return true;
}

void SendTicket(CloseTicket &item,const long expires_ms,const ulong started)
{
   item.finished=true;
   if(!CanClose() || !Current(expires_ms) || !PositionSelectByTicket(item.request.position)) return;
   const long side=PositionGetInteger(POSITION_TYPE);
   if(PositionGetString(POSITION_SYMBOL)!=item.request.symbol
      || MathAbs(PositionGetDouble(POSITION_VOLUME)-item.request.volume)>1e-8
      || (side==POSITION_TYPE_BUY && item.request.type!=ORDER_TYPE_SELL)
      || (side==POSITION_TYPE_SELL && item.request.type!=ORDER_TYPE_BUY)
      || (side!=POSITION_TYPE_BUY && side!=POSITION_TYPE_SELL)) return;
   MqlTick tick;
   if(!SymbolInfoTick(item.request.symbol,tick)) return;
   item.request.price=side==POSITION_TYPE_BUY ? tick.bid : tick.ask;
   if(!MathIsValidNumber(item.request.price) || item.request.price<=0) return;
   MqlTradeResult response={};
   item.dispatch_ms=(GetMicrosecondCount()-started)/1000.0;
   item.submitted=OrderSendAsync(item.request,response);
   item.request_id=response.request_id;
   item.retcode=response.retcode;
   item.finished=!item.submitted;
}

bool WriteReply(CloseBatch &batch)
{
   const string pending=directory+batch.id+".tmp";
   const int file=FileOpen(pending,FILE_WRITE|FILE_CSV|FILE_ANSI,9,CP_UTF8);
   if(file==INVALID_HANDLE) return false;
   bool wrote=FileWrite(file,"1",batch.id)>0;
   for(int i=0;i<batch.count && wrote;i++)
      wrote=FileWrite(file,(string)batch.tickets[i].request.position,batch.tickets[i].submitted ? "1" : "0",
                      (string)batch.tickets[i].retcode,(string)batch.tickets[i].order,
                      DoubleToString(batch.tickets[i].volume,8),DoubleToString(batch.tickets[i].price,12),
                      DoubleToString(batch.tickets[i].dispatch_ms,3))>0;
   FileClose(file);
   return wrote && FileMove(pending,0,directory+batch.id+".reply",FILE_REWRITE);
}

void FlushBatches(const ulong now)
{
   for(int i=0;i<MAX_BATCHES;i++)
   {
      if(batches[i].count==0) continue;
      bool complete=true;
      for(int j=0;j<batches[i].count;j++)
         if(!batches[i].tickets[j].finished) { complete=false; break; }
      if((complete || now-batches[i].started>=1500000) && WriteReply(batches[i]))
      {
         FileDelete(directory+batches[i].id+".claimed");
         batches[i].count=0;
      }
   }
}

void ReadInbox()
{
   if(!FileIsExist(directory+"wake")) return;
   int slot=0;
   while(slot<MAX_BATCHES && batches[slot].count>0) slot++;
   if(slot==MAX_BATCHES || !FileDelete(directory+"wake")) return;
   string name;
   const long finder=FileFindFirst(directory+"*.send",name);
   if(finder==INVALID_HANDLE) return;
   do
   {
      if(StringLen(name)!=69) continue;
      const string id=StringSubstr(name,0,64);
      if(!ValidId(id)) continue;
      const string claimed=directory+id+".claimed";
      // Claim before reading. A terminal restart never scans claimed files.
      if(!FileMove(directory+name,0,claimed,0)) continue;
      if(!LoadBatch(claimed,id,batches[slot])) { FileDelete(claimed); continue; }
      for(int i=0;i<batches[slot].count;i++)
         SendTicket(batches[slot].tickets[i],batches[slot].expires_ms,batches[slot].started);
      while(slot<MAX_BATCHES && batches[slot].count>0) slot++;
      if(slot==MAX_BATCHES)
      {
         const int wake=FileOpen(directory+"wake",FILE_WRITE|FILE_BIN|FILE_SHARE_READ|FILE_SHARE_WRITE);
         if(wake!=INVALID_HANDLE) FileClose(wake);
         break;
      }
   }
   while(FileFindNext(finder,name));
   FileFindClose(finder);
}

int OnInit()
{
   if(ExpectedAccount<=0 || ExpectedServer=="" || !AccountMatches()) return INIT_PARAMETERS_INCORRECT;
   directory="ISCCAsyncClose\\"+(string)ExpectedAccount+"\\";
   FolderCreate("ISCCAsyncClose");
   FolderCreate(directory);
   if(!EventSetMillisecondTimer(25)) return INIT_FAILED;
   return INIT_SUCCEEDED;
}

void OnDeinit(const int reason)
{
   EventKillTimer();
   if(ready) FileDelete(directory+"ready");
}

void OnTimer()
{
   const ulong now=GetMicrosecondCount();
   if(now-last_heartbeat>=1000000)
   {
      if(CanClose()) ready=SaveReady();
      else if(ready) { FileDelete(directory+"ready"); ready=false; }
      last_heartbeat=now;
   }
   FlushBatches(now);
   if(ready) ReadInbox();
}

void OnTradeTransaction(const MqlTradeTransaction &transaction,const MqlTradeRequest &request,const MqlTradeResult &result)
{
   if(transaction.type!=TRADE_TRANSACTION_REQUEST || !AccountMatches() || result.request_id==0) return;
   for(int i=0;i<MAX_BATCHES;i++)
      for(int j=0;j<batches[i].count;j++)
         if(batches[i].tickets[j].submitted && !batches[i].tickets[j].finished
            && batches[i].tickets[j].request_id==result.request_id
            && batches[i].tickets[j].request.position==request.position
            && batches[i].tickets[j].request.symbol==request.symbol
            && request.action==TRADE_ACTION_DEAL && batches[i].tickets[j].request.type==request.type
            && MathAbs(batches[i].tickets[j].request.volume-request.volume)<=1e-8)
         {
            batches[i].tickets[j].retcode=result.retcode;
            batches[i].tickets[j].order=result.order;
            batches[i].tickets[j].volume=result.volume;
            batches[i].tickets[j].price=result.price;
            batches[i].tickets[j].finished=result.retcode!=TRADE_RETCODE_PLACED;
            return;
         }
}
