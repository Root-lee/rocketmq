package org.apache.rocketmq.example.simple;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.*;

class OrderedConsumer {
    private static final Map<MessageQueue, Long> OFFSE_TABLE = new HashMap<MessageQueue, Long>();

    void receive() throws MQClientException {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("please_rename_unique_group_name_5");

        consumer.start();

        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("TopicTest1");

        List<MessageExt> list = new ArrayList<>();

        Object lock = new Object();

        while (true) {
            SINGLE_MQ:
            for (MessageQueue mq : mqs) {
                try {
                    PullResult pullResult = consumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq), 32);
                    System.out.printf("%s%n", pullResult);
                    putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            list.addAll(pullResult.getMsgFoundList());
                            list = sort(list, 0, list.size());
                            synchronized (lock) {
                                List<MessageExt> consumeList = new ArrayList<>();
                                Collections.copy(consumeList, list);
                                list.clear();
                                // consume(consumeList)
                            }
                            break;
                        case NO_MATCHED_MSG:
                            break;
                        case NO_NEW_MSG:
                            break SINGLE_MQ;
                        case OFFSET_ILLEGAL:
                            break;
                        default:
                            break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static long getMessageQueueOffset(MessageQueue mq) {
        Long offset = OFFSE_TABLE.get(mq);
        if (offset != null) {
            return offset;
        }
        return 0;
    }

    private static void putMessageQueueOffset(MessageQueue mq, long offset) {
        OFFSE_TABLE.put(mq, offset);
    }

    private List<MessageExt> sort(List<MessageExt> targetList, int low, int high) {
        int mid = (low + high) / 2;
        if(low < high){
            sort(targetList, low, mid);
            sort(targetList,mid + 1, high);
            merge(targetList, low, mid, high);
        }

        return targetList;
    }

    private void merge(List<MessageExt> originList, int low, int mid, int high) {
        List<MessageExt> temp = new ArrayList<>();
        int i = low;
        int j = mid + 1;

        while(i <= mid && j <= high){
            if(originList.get(i).getBornTimestamp() < originList.get(j).getBornTimestamp()){
                temp.add(originList.get(i++));
            }else{
                temp.add(originList.get(j++));
            }
        }

        while(i <= mid){
            temp.add(originList.get(i++));
        }

        while(j <= high){
            temp.add(originList.get(j++));
        }

        for(int x = 0; x < temp.size(); x++){
            originList.set(x, temp.get(x));
        }
    }
}
