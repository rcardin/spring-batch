/*
 * Copyright 2018-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.batch.integration.chunk;

import org.junit.Assert;
import org.junit.Test;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Mahmoud Ben Hassine
 */
public class RemoteChunkingWorkerBuilderTest {

	private ItemProcessor<String, String> itemProcessor = new PassThroughItemProcessor<>();
	private ItemWriter<String> itemWriter = items -> { };

	@Test
	public void itemProcessorMustNotBeNull() {
		// when
		final Exception expectedException = Assert.assertThrows(IllegalArgumentException.class,
				() -> new RemoteChunkingWorkerBuilder<String, String>().itemProcessor(null).build());

		// then
		assertThat(expectedException).hasMessage("itemProcessor must not be null");
	}

	@Test
	public void itemWriterMustNotBeNull() {
		// when
		final Exception expectedException = Assert.assertThrows(IllegalArgumentException.class,
				() -> new RemoteChunkingWorkerBuilder<String, String>().itemWriter(null).build());

		// then
		assertThat(expectedException).hasMessage("itemWriter must not be null");
	}

	@Test
	public void inputChannelMustNotBeNull() {
		// when
		final Exception expectedException = Assert.assertThrows(IllegalArgumentException.class,
				() -> new RemoteChunkingWorkerBuilder<String, String>().inputChannel(null).build());

		// then
		assertThat(expectedException).hasMessage("inputChannel must not be null");
	}

	@Test
	public void outputChannelMustNotBeNull() {
		// when
		final Exception expectedException = Assert.assertThrows(IllegalArgumentException.class,
				() -> new RemoteChunkingWorkerBuilder<String, String>().outputChannel(null).build());

		// then
		assertThat(expectedException).hasMessage("outputChannel must not be null");
	}

	@Test
	public void testMandatoryItemWriter() {
		// given
		RemoteChunkingWorkerBuilder<String, String> builder = new RemoteChunkingWorkerBuilder<>();

		// when
		final Exception expectedException = Assert.assertThrows(IllegalArgumentException.class, builder::build);

		// then
		assertThat(expectedException).hasMessage("An ItemWriter must be provided");
	}

	@Test
	public void testMandatoryInputChannel() {
		// given
		RemoteChunkingWorkerBuilder<String, String> builder = new RemoteChunkingWorkerBuilder<String, String>()
				.itemWriter(items -> { });

		// when
		final Exception expectedException = Assert.assertThrows(IllegalArgumentException.class, builder::build);

		// then
		assertThat(expectedException).hasMessage("An InputChannel must be provided");
	}

	@Test
	public void testMandatoryOutputChannel() {
		// given
		RemoteChunkingWorkerBuilder<String, String> builder = new RemoteChunkingWorkerBuilder<String, String>()
				.itemWriter(items -> { })
				.inputChannel(new DirectChannel());


		// when
		final Exception expectedException = Assert.assertThrows(IllegalArgumentException.class, builder::build);

		// then
		assertThat(expectedException).hasMessage("An OutputChannel must be provided");
	}

	@Test
	public void testIntegrationFlowCreation() {
		// given
		DirectChannel inputChannel = new DirectChannel();
		DirectChannel outputChannel = new DirectChannel();
		RemoteChunkingWorkerBuilder<String, String> builder = new RemoteChunkingWorkerBuilder<String, String>()
				.itemProcessor(this.itemProcessor)
				.itemWriter(this.itemWriter)
				.inputChannel(inputChannel)
				.outputChannel(outputChannel);

		// when
		IntegrationFlow integrationFlow = builder.build();

		// then
		Assert.assertNotNull(integrationFlow);
	}

}
