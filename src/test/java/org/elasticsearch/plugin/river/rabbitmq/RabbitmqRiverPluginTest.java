package org.elasticsearch.plugin.river.rabbitmq;

import org.elasticsearch.river.RiversModule;
import org.elasticsearch.river.rabbitmq.RabbitmqRiverModule;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;


public class RabbitmqRiverPluginTest {

    RabbitmqRiverPlugin rabbitmqRiverPlugin;

    @BeforeClass
    public void oneTimeSetUp() {
        rabbitmqRiverPlugin = new RabbitmqRiverPlugin();
    }

    @Test
    public void nameTest() throws IOException, InterruptedException {
        assertThat(rabbitmqRiverPlugin.name(), equalTo("river-rabbitmq"));
    }

    @Test
    public void descriptionTest() throws IOException, InterruptedException {
        assertThat(rabbitmqRiverPlugin.description(), equalTo("River RabbitMQ Plugin"));
    }

    @Test
    public void onModuleTest() throws IOException, InterruptedException {
        RiversModule mockRiversModule = mock(RiversModule.class);
        rabbitmqRiverPlugin.onModule(mockRiversModule);
        verify(mockRiversModule).registerRiver("rabbitmq", RabbitmqRiverModule.class);
    }

}
