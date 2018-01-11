using log4net;
using NetMQ;
using NetMQ.Sockets;
using OMSNF_FEP.ServiceManager.MSG_Processor;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace OMSNF_FEP.ServiceManager
{
    class SUB_Services:ExitNodeService
    {
        private static SUB_Services instance=new SUB_Services();
        public static SUB_Services Instance
        {
            get { return instance; }
        }
        private static readonly ILog logger = LogManager.GetLogger(typeof(SUB_Services));
        static SubscriberSocket sub_socket = new SubscriberSocket();
        BlockingCollection<List<byte[]>> sub_block = new BlockingCollection<List<byte[]>>();
        private CancellationTokenSource cts = new CancellationTokenSource();
        private CancellationToken ct;

        Dictionary<string, List<string>> protobf_msgtype_topiclist = new Dictionary<string, List<string>>();
        List<string> iplist = new List<string>();
        long subCount = 0;
        private SUB_Services() {
            StartSublisherService();
        }
        public void AddSubService(string ipport,string newtopic)
        {
            AddTopic(newtopic);
            if (!iplist.Contains(ipport))
            {
                sub_socket.Connect(ipport);
                iplist.Add(ipport);
            }
            
            //            lock (sub_socket)
            //            {
            //                if (sub_socket == null)// && newtopic.Contains(Jsunfutures.Messages.MessageFlag.MsgUserdata.ToString()))
            //#if Debug

            //                    StartSublisherService("tcp://10.99.103.134:10999");
            //#else
            //                StartSublisherService(ipport);
            //#endif

            //                else
            //                {
            //#if Debug
            //                    sub_socket.Connect("tcp://10.99.103.134:10999");                    

            //#else
            //                sub_socket.Connect(ipport);
            //#endif

            //}
            //    }
        }

        //public void StartSublisherService(string ipport)
        public void StartSublisherService()
        {
            try
            {
                Task.Factory.StartNew(() =>
                {
                    //logger.Info(ipport + " start sub");
                    logger.Info(" start sub");
                    string concernMsg = Properties.Settings.Default.SUB_Concern;                   
                    //using (sub_socket = new SubscriberSocket(ipport))
                    {
                        //sub_socket.SubscribeToAnyTopic();
                        MSG_FEPQueryProcessor frpquery_processor = MSG_FEPQueryProcessor.Instance;
                        ct = cts.Token;
                        while (true)
                        {
                            try
                            {
                                Console.WriteLine("Sub start Recv data ! ");
                                //List<byte[]> l = sub_socket.ReceiveMultipartBytes();
                                List<byte[]> k = sub_socket.ReceiveMultipartBytes();
                                sub_block.Add(k);
                                Console.WriteLine("Sub start Recv data !2");
                                //while (sub_block.Count != 0)
                                //{
                                //    List<byte[]> l= sub_block.Take();


                                //    string topic = Encoding.UTF8.GetString((byte[])l[0]);
                                //    logger.Info("FEP Receive a Subscribed topic " + topic);
                                //    string msgtype = topic.Split('.')[2];
                                //    Jsunfutures.Messages.MessageFlag mf;
                                //    Enum.TryParse(msgtype, out mf);
                                //    byte[][] a = l.ToArray<byte[]>();
                                //    //Google.Protobuf.CodedInputStream oGPC = new Google.Protobuf.CodedInputStream(a[1]);

                                //    //if (msgtype.Equals(concernMsg))
                                //    //{
                                //    //    Jsunfutures.Messages.FEPQuery oMsg = new Jsunfutures.Messages.FEPQuery();
                                //    //    oMsg.MergeFrom(oGPC);
                                //    //    if (AAWatch.Instance.aaflag == Jsunfutures.Messages.OmsAAFlag.OmsaafStandby) continue;
                                //    //    frpquery_processor.Process(oMsg);
                                //    //    logger.Info("SUB<- " + (++subCount) + " topic[" + topic + "] " + oMsg);
                                //    //}
                                //    //else if (msgtype.Equals(userMsg))
                                //    //{
                                //    //    Jsunfutures.Messages.SSO oMsg = new Jsunfutures.Messages.SSO();
                                //    //    oMsg.MergeFrom(oGPC);
                                //    //    if (AAWatch.Instance.aaflag == Jsunfutures.Messages.OmsAAFlag.OmsaafStandby) continue;
                                //    //    frpquery_processor.Process(oMsg);
                                //    //    logger.Info("SUB<- " + (++subCount) + " topic[" + topic + "] " + oMsg);
                                //    //}

                                //    switch (mf)
                                //    {
                                //        case Jsunfutures.Messages.MessageFlag.MsgFepquery:
                                //            Jsunfutures.Messages.FEPQuery oMsg = Jsunfutures.Messages.FEPQuery.Parser.ParseFrom(a[1]);
                                //            logger.Info("SUB<- " + (++subCount) + " topic[" + topic + "] " + oMsg.ToString());
                                //            logger.Info("OmsASModeFlag is " + AAWatch.Instance.aaflag);
                                //            if (AAWatch.Instance.aaflag == Jsunfutures.Messages.OmsASModeFlag.OmsasModeStandby) continue;
                                //            Task.Run(() => frpquery_processor.Process(oMsg));
                                //            break;
                                //        case Jsunfutures.Messages.MessageFlag.MsgUserdata:
                                //            Jsunfutures.Messages.SSO ssoMsg = Jsunfutures.Messages.SSO.Parser.ParseFrom(a[1]);
                                //            logger.Info("SUB<- " + (++subCount) + " topic[" + topic + "] " + ssoMsg.ToString());
                                //            logger.Info("OmsASModeFlag is " + AAWatch.Instance.aaflag);
                                //            if (AAWatch.Instance.aaflag == Jsunfutures.Messages.OmsASModeFlag.OmsasModeStandby) continue;
                                //            Task.Run(() => frpquery_processor.Process(ssoMsg));
                                //            break;
                                //    }
                                //    logger.Info("Sub End Recv data ! ");
                                //}
                            }
                            catch (Exception e) { Console.WriteLine("Sub Recv data Error! "); logger.Error(e); }
                        }
                    }
                }).ConfigureAwait(false);
                
                Task.Factory.StartNew(() =>
                {
                    MSG_FEPQueryProcessor frpquery_processor = MSG_FEPQueryProcessor.Instance;
                    ct = cts.Token;
                    while (true)
                    {
                        Console.WriteLine("StartPorcessorService2");
                        if (sub_block.Count != 0)
                        {
                            List<byte[]> l = sub_block.Take(ct);
                            string topic = Encoding.UTF8.GetString((byte[])l[0]);
                            logger.Info("FEP Receive a Subscribed topic " + topic);
                            string msgtype = topic.Split('.')[2];
                            Jsunfutures.Messages.MessageFlag mf;
                            Enum.TryParse(msgtype, out mf);
                            byte[][] a = l.ToArray<byte[]>();
                            switch (mf)
                            {
                                case Jsunfutures.Messages.MessageFlag.MsgFepquery:
                                    Jsunfutures.Messages.FEPQuery oMsg = Jsunfutures.Messages.FEPQuery.Parser.ParseFrom(a[1]);
                                    logger.Info("SUB<- " + (++subCount) + " topic[" + topic + "] " + oMsg.ToString());
                                    logger.Info("OmsASModeFlag is " + AAWatch.Instance.aaflag);
                                    if (AAWatch.Instance.aaflag == Jsunfutures.Messages.OmsASModeFlag.OmsasModeStandby) continue;
                                    Task.Run(() => frpquery_processor.Process(oMsg));
                                    break;
                                case Jsunfutures.Messages.MessageFlag.MsgUserdata:
                                    Jsunfutures.Messages.SSO ssoMsg = Jsunfutures.Messages.SSO.Parser.ParseFrom(a[1]);
                                    logger.Info("SUB<- " + (++subCount) + " topic[" + topic + "] " + ssoMsg.ToString());
                                    logger.Info("OmsASModeFlag is " + AAWatch.Instance.aaflag);
                                    if (AAWatch.Instance.aaflag == Jsunfutures.Messages.OmsASModeFlag.OmsasModeStandby) continue;
                                    Task.Run(() => frpquery_processor.Process(ssoMsg));
                                    break;
                            }
                            logger.Info("Sub End Recv data ! ");
                        }
                    }
                Thread.Sleep(99);
                },cts.Token);
            }
            catch (Exception e)
            {
                logger.Error(e);
            }
            Console.WriteLine("End Sub Services ");
        }
        public void AddTopic(string topic)
        {
            string[] ssmrt = topic.Split('.');
            string msgtype = ssmrt[2];
            if (!protobf_msgtype_topiclist.ContainsKey(msgtype))
            {
                 List<string> topicList = new List<string>();
                topicList.Add(topic);
                sub_socket.Subscribe(topic);
                protobf_msgtype_topiclist.Add(msgtype, topicList);
                logger.Info("SUB 端接收的訊息 [" + msgtype + "] 已加入成功,  topic[" + topic + "]將透過 此SUB 接收");
            }
            else
            {
                List<string> topicList = protobf_msgtype_topiclist[msgtype];
                if (!topicList.Contains(topic))
                {
                    topicList.Add(topic);
                    sub_socket.Subscribe(topic);
                    logger.Info("SUB 端接收的訊息 [" + msgtype + "] 已加入成功,  topic[" + topic + "]將透過 此SUB 接收");
                } 
            } 
        }

        public void ExitNodeService()
        {
            cts.Cancel();
        } 
    }
}
