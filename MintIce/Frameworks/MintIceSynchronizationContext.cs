// Zlib license
//
// Copyright (c) 2021 Sinoa
//
// This software is provided 'as-is', without any express or implied warranty.
// In no event will the authors be held liable for any damages arising from the use of this software.
// Permission is granted to anyone to use this software for any purpose,
// including commercial applications, and to alter it and redistribute it freely,
// subject to the following restrictions:
//
// 1. The origin of this software must not be misrepresented; you must not claim that you wrote the original software.
//    If you use this software in a product, an acknowledgment in the product documentation would be appreciated but is not required.
// 2. Altered source versions must be plainly marked as such, and must not be misrepresented as being the original software.
// 3. This notice may not be removed or altered from any source distribution.

using System;
using System.Collections.Generic;
using System.Threading;

namespace Sinoalmond.MintIce.Frameworks
{
    public sealed class MintIceSynchronizationContext : SynchronizationContext
    {
        private readonly object syncObject;
        private readonly int myThreadId;
        private Queue<Message> frontMessageQueue;
        private Queue<Message> backMessageQueue;



        public MintIceSynchronizationContext(out Action messagePumpHandler)
        {
            syncObject = new object();
            frontMessageQueue = new Queue<Message>();
            backMessageQueue = new Queue<Message>();
            myThreadId = Thread.CurrentThread.ManagedThreadId;
            messagePumpHandler = ProcessMessage;
        }


        public override void Send(SendOrPostCallback d, object state)
        {
            if (Thread.CurrentThread.ManagedThreadId == myThreadId)
            {
                d(state);
                return;
            }

            var waitHandle = new ManualResetEvent(false);
            EnqueueMessage(new Message(d, state, waitHandle));
            waitHandle.WaitOne();
        }


        public override void Post(SendOrPostCallback d, object state)
        {
            EnqueueMessage(new Message(d, state, null));
        }


        private void EnqueueMessage(in Message message)
        {
            lock (syncObject)
            {
                backMessageQueue.Enqueue(message);
            }
        }


        private void ProcessMessage()
        {
            var frontMessageQueue = SwitchQueue();
            while (frontMessageQueue.Count > 0)
            {
                frontMessageQueue.Dequeue().Invoke();
            }
        }


        private Queue<Message> SwitchQueue()
        {
            lock (syncObject)
            {
                var x = frontMessageQueue;
                frontMessageQueue = backMessageQueue;
                backMessageQueue = x;
            }

            return frontMessageQueue;
        }



        private readonly struct Message
        {
            private readonly ManualResetEvent waitHandle;
            private readonly SendOrPostCallback callback;
            private readonly object state;



            public Message(SendOrPostCallback callback, object state, ManualResetEvent waitHandle)
            {
                this.waitHandle = waitHandle;
                this.callback = callback;
                this.state = state;
            }


            public void Invoke()
            {
                try
                {
                    callback(state);
                }
                finally
                {
                    waitHandle?.Set();
                }
            }
        }
    }
}