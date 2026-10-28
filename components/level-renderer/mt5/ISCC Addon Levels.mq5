#property copyright "ISCC"
#property version "1.17"
#property indicator_chart_window
#property indicator_plots 0

// Only owned chart objects and two local files are used. No trading functions.
const string PREFIX="ISCC_ADDON_TV_";
const string INPUT_FILE="iscc-addon-levels.csv";
const string ACK_FILE="iscc-addon-levels-ack.csv";

struct Marker {
   string id,revision,symbol,timeframe,tool,side;
   datetime time;
   double price;
   color lineColor;
};

color FromRgb(long rgb) {
   return (color)(((rgb & 255)<<16) | (rgb & 65280) | ((rgb>>16) & 255));
}

long ToRgb(color value) {
   long native=(long)value;
   return ((native & 255)<<16) | (native & 65280) | ((native>>16) & 255);
}

bool ReadLine(int handle,string &fields[]) {
   if(FileIsEnding(handle)) return false;
   string line=FileReadString(handle);
   StringReplace(line,"\r","");
   return StringSplit(line,'\t',fields)>0;
}

// Reuse decoded rows only on a timer turn that cannot publish an ACK. Every
// actual object audit still reads and validates the complete current file.
Marker inputMarkers[];
string cachedInputGeneration="";
int cachedInputProtocol=0;
bool cachedInputValid=false;

bool Load(string &generation,int &protocol,bool allowCache=false) {
   int handle=FileOpen(INPUT_FILE,FILE_READ|FILE_TXT|FILE_ANSI|FILE_SHARE_READ,0,CP_UTF8);
   if(handle==INVALID_HANDLE) {cachedInputValid=false;return false;}
   string parts[];
   bool valid=ReadLine(handle,parts) && ArraySize(parts)==4 && parts[0]=="ISCC_CANDLE_LEVELS" && (parts[1]=="1" || parts[1]=="2");
   protocol=valid ? (int)StringToInteger(parts[1]) : 0;
   int count=valid ? (int)StringToInteger(parts[3]) : -1;
   if(!valid || count<0 || count>2000) {cachedInputValid=false;FileClose(handle);return false;}
   generation=parts[2];
   if(StringLen(generation)!=64) {cachedInputValid=false;FileClose(handle);return false;}
   if(allowCache && cachedInputValid && generation==cachedInputGeneration
      && protocol==cachedInputProtocol && count==ArraySize(inputMarkers)) {
      FileClose(handle);
      return true;
   }
   ArrayResize(inputMarkers,count);
   for(int i=0;i<count;i++) {
      if(!ReadLine(handle,parts) || ArraySize(parts)!=(protocol==2 ? 9 : 8)) {valid=false;break;}
      inputMarkers[i].id=parts[0];inputMarkers[i].revision=parts[1];inputMarkers[i].symbol=parts[2];
      inputMarkers[i].timeframe=parts[3];inputMarkers[i].time=(datetime)StringToInteger(parts[4]);
      inputMarkers[i].price=StringToDouble(parts[5]);inputMarkers[i].tool=parts[6];inputMarkers[i].side=parts[7];
      long rgb=protocol==2 ? StringToInteger(parts[8]) : (inputMarkers[i].side=="high" ? 16737095 : 49151);
      if(rgb<0 || rgb>16777215) {valid=false;break;}
      inputMarkers[i].lineColor=FromRgb(rgb);
      if(StringLen(inputMarkers[i].id)!=24 || inputMarkers[i].price<=0 || !MathIsValidNumber(inputMarkers[i].price)
         || inputMarkers[i].time<=0 || (inputMarkers[i].tool!="line" && inputMarkers[i].tool!="ray")
         || (inputMarkers[i].side!="high" && inputMarkers[i].side!="low" && inputMarkers[i].side!="price")) {valid=false;break;}
   }
   if(valid) valid=ReadLine(handle,parts) && ArraySize(parts)==3 && parts[0]=="END" && parts[1]==generation && (int)StringToInteger(parts[2])==count;
   FileClose(handle);
   cachedInputValid=valid;
   if(valid) {
      cachedInputGeneration=generation;cachedInputProtocol=protocol;
   }
   return valid;
}

// ACK files expire after five seconds in the receiver. Refresh them only after
// a completed verification, every two seconds for unchanged charts. Never keep
// an old ready result alive by rewriting it on a skipped timer turn.
const ulong VERIFY_INTERVAL_MS=2000;
const ulong CLEANUP_INTERVAL_MS=30000;
string verifiedGeneration="",verifiedTopology="";
int verifiedProtocol=0;
bool verified=false,cleanupVerified=false;
ulong lastVerifiedAt=0,lastCleanupAt=0;

bool Contains(Marker &markers[],string name,string symbol) {
   for(int i=0;i<ArraySize(markers);i++)
      if(PREFIX+markers[i].id==name && markers[i].symbol==symbol) return true;
   return false;
}

bool EnsureInteger(long chart,string name,ENUM_OBJECT_PROPERTY_INTEGER property,long expected,bool &changed) {
   long actual;
   if(!ObjectGetInteger(chart,name,property,0,actual)) return false;
   if(actual==expected) return true;
   if(!ObjectSetInteger(chart,name,property,expected)) return false;
   changed=true;
   return ObjectGetInteger(chart,name,property,0,actual) && actual==expected;
}

bool EnsurePrice(long chart,string name,double expected,bool &changed) {
   double actual;
   if(!ObjectGetDouble(chart,name,OBJPROP_PRICE,0,actual)) return false;
   if(MathAbs(actual-expected)<0.00000001) return true;
   if(!ObjectSetDouble(chart,name,OBJPROP_PRICE,0,expected)) return false;
   changed=true;
   return ObjectGetDouble(chart,name,OBJPROP_PRICE,0,actual) && MathAbs(actual-expected)<0.00000001;
}

bool EnsureTooltip(long chart,string name,string expected,bool &changed) {
   string actual;
   if(!ObjectGetString(chart,name,OBJPROP_TOOLTIP,0,actual)) return false;
   if(actual==expected) return true;
   if(!ObjectSetString(chart,name,OBJPROP_TOOLTIP,expected)) return false;
   changed=true;
   return ObjectGetString(chart,name,OBJPROP_TOOLTIP,0,actual) && actual==expected;
}

bool Draw(long chart,Marker &marker,bool &changed) {
   string name=PREFIX+marker.id;
   // The user chose native full horizontal levels in MT5 for both TV tools.
   // MT5 itself draws their prices on its standard right-hand price scale.
   // Source candle/timeframe/side and the exact confirmed price remain intact.
   int window=ObjectFind(chart,name);
   if(window>=0) {
      long type;
      if(!ObjectGetInteger(chart,name,OBJPROP_TYPE,0,type)) return false;
      if(window!=0 || type!=OBJ_HLINE) {
         if(!ObjectDelete(chart,name)) return false;
         changed=true;
         if(ObjectFind(chart,name)>=0) return false;
         window=-1;
      }
   }
   if(window<0) {
      if(!ObjectCreate(chart,name,OBJ_HLINE,0,0,marker.price)) return false;
      changed=true;
      // A queued ObjectCreate is not confirmation of an applied level.
      if(ObjectFind(chart,name)!=0) return false;
   }
   // Verify actual properties on each audit; write only changed values. This
   // repairs deleted/edited owned objects without repainting unchanged charts.
   return EnsurePrice(chart,name,marker.price,changed)
      && EnsureInteger(chart,name,OBJPROP_COLOR,marker.lineColor,changed)
      && EnsureInteger(chart,name,OBJPROP_WIDTH,1,changed)
      && EnsureInteger(chart,name,OBJPROP_STYLE,STYLE_SOLID,changed)
      && EnsureInteger(chart,name,OBJPROP_BACK,false,changed)
      && EnsureInteger(chart,name,OBJPROP_TIMEFRAMES,OBJ_ALL_PERIODS,changed)
      && EnsureInteger(chart,name,OBJPROP_SELECTABLE,false,changed)
      && EnsureInteger(chart,name,OBJPROP_SELECTED,false,changed)
      && EnsureInteger(chart,name,OBJPROP_HIDDEN,false,changed)
      && EnsureTooltip(chart,name,"TV → MT5 | "+marker.timeframe+" | "+marker.side+" | "+TimeToString(marker.time),changed);
}

int OnInit() {
   verified=false;cleanupVerified=false;
   cachedInputValid=false;ArrayResize(inputMarkers,0);
   if(!EventSetTimer(1)) return INIT_FAILED;
   // A bounded load receipt lets the installer verify the running version.
   int status=FileOpen("iscc-addon-levels-indicator-status.csv",FILE_WRITE|FILE_TXT|FILE_ANSI|FILE_SHARE_READ,0,CP_UTF8);
   if(status!=INVALID_HANDLE) {
      FileWriteString(status,"ISCC_CANDLE_LEVELS_INDICATOR\t1.17\texisting-charts-only\t"+IntegerToString(ChartID())+"\n");
      FileClose(status);
   }
   return INIT_SUCCEEDED;
}
void OnDeinit(const int reason) {EventKillTimer();}
int OnCalculate(const int rates_total,const int prev_calculated,const datetime &time[],
   const double &open[],const double &high[],const double &low[],const double &close[],
   const long &tick_volume[],const long &volume[],const int &spread[]) {return rates_total;}

void OnTimer() {
   long charts[];string symbols[];bool changed[];
   string topology="";
   for(long chart=ChartFirst();chart>=0;chart=ChartNext(chart)) {
      int index=ArraySize(charts);
      ArrayResize(charts,index+1);ArrayResize(symbols,index+1);ArrayResize(changed,index+1);
      charts[index]=chart;symbols[index]=ChartSymbol(chart);changed[index]=false;
      topology+=IntegerToString(chart)+":"+IntegerToString(StringLen(symbols[index]))+":"
         +symbols[index]+":"+IntegerToString((long)ChartPeriod(chart))+";";
   }
   ulong now=GetTickCount64();
   string generation;int protocol;
   bool allowInputCache=verified && topology==verifiedTopology && now-lastVerifiedAt<VERIFY_INTERVAL_MS;
   if(!Load(generation,protocol,allowInputCache)) {verified=false;return;}
   bool stateChanged=generation!=verifiedGeneration || protocol!=verifiedProtocol || topology!=verifiedTopology;
   if(verified && !stateChanged && now-lastVerifiedAt<VERIFY_INTERVAL_MS) return;
   verified=false;
   bool cleanupReady=true;
   // Full object enumeration is needed for changed snapshots/topology and a
   // bounded periodic sweep, rather than for every unchanged timer turn.
   if(stateChanged || !cleanupVerified || now-lastCleanupAt>=CLEANUP_INTERVAL_MS) {
      for(int c=0;c<ArraySize(charts);c++) {
         long chart=charts[c];
         for(int j=ObjectsTotal(chart)-1;j>=0;j--) {
            string name=ObjectName(chart,j);
            if(StringFind(name,PREFIX)==0 && !Contains(inputMarkers,name,symbols[c])) {
               if(!ObjectDelete(chart,name) || ObjectFind(chart,name)>=0) cleanupReady=false;
               changed[c]=true;
            }
         }
      }
      cleanupVerified=cleanupReady;
      if(cleanupReady) lastCleanupAt=GetTickCount64();
   }
   if(!cleanupReady) {
      for(int c=0;c<ArraySize(charts);c++) if(changed[c]) ChartRedraw(charts[c]);
      return;
   }
   bool found[],ready[];
   ArrayResize(found,ArraySize(inputMarkers));ArrayResize(ready,ArraySize(inputMarkers));
   ArrayInitialize(found,false);ArrayInitialize(ready,true);
   for(int c=0;c<ArraySize(charts);c++) {
      for(int i=0;i<ArraySize(inputMarkers);i++) {
         if(symbols[c]!=inputMarkers[i].symbol) continue;
         found[i]=true;
         bool objectChanged=false;
         if(!Draw(charts[c],inputMarkers[i],objectChanged)) ready[i]=false;
         if(objectChanged) changed[c]=true;
      }
      // One redraw for the whole batch, and none for unchanged levels.
      if(changed[c]) ChartRedraw(charts[c]);
   }
   int output=FileOpen(ACK_FILE,FILE_WRITE|FILE_TXT|FILE_ANSI|FILE_SHARE_READ,0,CP_UTF8);
   if(output==INVALID_HANDLE) return;
   bool outputReady=FileWriteString(output,"ISCC_CANDLE_LEVELS_ACK\t"+IntegerToString(protocol)+"\t"+generation+"\t"+IntegerToString(ArraySize(inputMarkers))+"\n")>0;
   bool passReady=true;
   for(int i=0;i<ArraySize(inputMarkers);i++) {
      // A closed/missing chart stays closed. Its marker remains pending until
      // the user opens a matching chart; rendering never creates chart tabs.
      int digits=(int)SymbolInfoInteger(inputMarkers[i].symbol,SYMBOL_DIGITS);
      string row=inputMarkers[i].id+"\t"+inputMarkers[i].revision+"\t"+inputMarkers[i].symbol+"\t"
         +(found[i] && ready[i] ? "ready" : "pending")+"\t"+DoubleToString(inputMarkers[i].price,digits);
      if(protocol==2) row+="\t"+IntegerToString(ToRgb(inputMarkers[i].lineColor));
      if(FileWriteString(output,row+"\n")==0) outputReady=false;
      if(found[i] && !ready[i]) passReady=false;
   }
   if(FileWriteString(output,"END\t"+generation+"\t"+IntegerToString(ArraySize(inputMarkers))+"\n")==0) outputReady=false;
   FileClose(output);
   if(!outputReady) return;
   verifiedGeneration=generation;verifiedProtocol=protocol;verifiedTopology=topology;
   lastVerifiedAt=GetTickCount64();
   verified=passReady;
}
