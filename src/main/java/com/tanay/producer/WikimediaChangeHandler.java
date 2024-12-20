package com.tanay.producer;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements BackgroundEventHandler
{
    KafkaProducer<String, String> kafkaProduce;
    String topic;
    private final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getName());


    public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProduce, String topic)
    {
        this.kafkaProduce = kafkaProduce;
        this.topic = topic;
    }

    @Override
    public void onOpen()
    {
        // nothing here
    }

    @Override
    public void onClosed()
    {
        kafkaProduce.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent)
    {
        log.info(messageEvent.getData());
        // asynchronous
        kafkaProduce.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String s)
    {
        // nothing here
    }

    @Override
    public void onError(Throwable throwable)
    {
        log.error("Error in Stream Reading", throwable);
    }
}
