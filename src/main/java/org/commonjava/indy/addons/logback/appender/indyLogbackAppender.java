package org.commonjava.indy.addons.logback.appender;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;

public class indyLogbackAppender extends UnsynchronizedAppenderBase<ILoggingEvent>
{

    /**
     * Name of a server instance.
     * */
    private String jbossServerName;
    /**
     * Address to which is a server instance bound.
     * */
    private String jbossBindAddress;

    private Settings settings;

    /**
     * A name of elasticsearch cluster.
     * */
    private String clusterName;

    /**
     * An address of a cluster.
     * */
    private String transportAddress;

    /**
     * A port of an address.

     * */
    private Integer port;

    /**
     * An index to which logs are bound.
     * */
    private String index;

    /**
     * A type of an index.
     * */
    private String indexType;

    /**
     * Client which communicates with elascticsearch cluster.
     * */
    private volatile TransportClient client;

    public Settings getSettings()
    {
        return settings;
    }

    public void setSettings( Settings settings )
    {
        this.settings = settings;
    }

    public String getClusterName()
    {
        return clusterName;
    }

    public void setClusterName( String clusterName )
    {
        this.clusterName = clusterName;
    }

    public String getTransportAddress()
    {
        return transportAddress;
    }

    public void setTransportAddress( String transportAddress )
    {
        this.transportAddress = transportAddress;
    }

    public Integer getPort()
    {
        return port;
    }

    public void setPort( Integer port )
    {
        this.port = port;
    }

    public String getIndex()
    {
        return index;
    }

    public void setIndex( String index )
    {
        this.index = index;
    }

    public String getIndexType()
    {
        return indexType;
    }

    public void setIndexType( String indexType )
    {
        this.indexType = indexType;
    }

    public TransportClient getClient()
    {
        return client;
    }

    public void setClient( TransportClient client )
    {
        this.client = client;
    }

    protected void append( ILoggingEvent eventObject )
    {
        try
        {
            initClient();
            try
            {
                this.client.prepareIndex( this.index, this.indexType ).setSource( serializeToJson( eventObject ) ).execute();
            }
            catch ( final IOException e )
            {
                e.printStackTrace();
            }
        }catch ( Exception e )
        {

        }
    }
    /**
     * Serializes log record to JSON format using {@link XContentBuilder}.
     * @param eventObject eventObject to be serialized
     * @return serialized log record
     * */
    private XContentBuilder serializeToJson(ILoggingEvent eventObject ) throws IOException
    {
        final XContentBuilder builder = XContentFactory.jsonBuilder()
                                                       .startObject()
                                                       .field("timestamp", new Date( eventObject.getTimeStamp()))
                                                       .field("bind.address", "")
                                                       .field("server.name", "")
                                                       .field("level", eventObject.getLevel().toString())
                                                       .field("loggerName", eventObject.getLoggerName())
                                                       .field("message", eventObject.getMessage());

        final String name = eventObject.getThrowableProxy() == null ? "null" : eventObject.getThrowableProxy().getClass().getName();
        final String message = eventObject.getThrowableProxy() == null ? "null" : eventObject.getThrowableProxy().getMessage();

        builder.startObject("thrown")
               .field("name", name)
               .field("message", message)
               .endObject();
        builder.endObject();

        return builder;
    }

    private void initClient() throws UnknownHostException
    {
        if ( this.client == null )
        {
            synchronized ( this )
            {
                if ( this.client == null )
                {
                    if ( this.clusterName == null )
                    {
                        throw new IllegalStateException( "Cluster name cannot be null." );
                    }

                    if ( this.transportAddress == null )
                    {
                        throw new IllegalStateException( "Transport address cannot be null." );
                    }

                    if ( this.port == null )
                    {
                        throw new IllegalStateException( "Port cannot be null." );
                    }

                    if ( index == null )
                    {
                        throw new IllegalStateException( "Index cannot be null." );
                    }

                    if ( indexType == null )
                    {
                        throw new IllegalStateException( "Index type cannot be null." );
                    }
                    this.settings = Settings.builder()
                                            .put( "cluster.name", this.clusterName )
                                            .put( "name", "Wildfly Log Node" )
                                            .build();

                    this.client = new PreBuiltTransportClient( settings ).addTransportAddress(
                                    new InetSocketTransportAddress( InetAddress.getByName( this.transportAddress ),
                                                                    this.port ) );
                }
            }

        }
    }
}