package io.quarkus.hazelcast.client.deployment;

import com.hazelcast.client.LoadBalancer;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.client.config.ClientFlakeIdGeneratorConfig;
import com.hazelcast.client.config.ClientReliableTopicConfig;
import com.hazelcast.client.impl.ClientExtension;
import com.hazelcast.client.impl.protocol.task.topic.TopicAddMessageListenerMessageTask;
import com.hazelcast.client.impl.proxy.ClientCardinalityEstimatorProxy;
import com.hazelcast.client.impl.proxy.ClientDurableExecutorServiceProxy;
import com.hazelcast.client.impl.proxy.ClientExecutorServiceProxy;
import com.hazelcast.client.impl.proxy.ClientFlakeIdGeneratorProxy;
import com.hazelcast.client.impl.proxy.ClientListProxy;
import com.hazelcast.client.impl.proxy.ClientMultiMapProxy;
import com.hazelcast.client.impl.proxy.ClientPNCounterProxy;
import com.hazelcast.client.impl.proxy.ClientQueueProxy;
import com.hazelcast.client.impl.proxy.ClientReplicatedMapProxy;
import com.hazelcast.client.impl.proxy.ClientRingbufferProxy;
import com.hazelcast.client.impl.proxy.ClientScheduledExecutorProxy;
import com.hazelcast.client.impl.proxy.ClientSetProxy;
import com.hazelcast.client.impl.proxy.ClientTopicProxy;
import com.hazelcast.client.impl.proxy.txn.xa.XAResourceProxy;
import com.hazelcast.client.impl.spi.ClientProxyFactory;
import com.hazelcast.client.util.AbstractLoadBalancer;
import com.hazelcast.client.util.RandomLB;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.collection.ItemListener;
import com.hazelcast.config.replacer.EncryptionReplacer;
import com.hazelcast.config.replacer.PropertyReplacer;
import com.hazelcast.config.replacer.spi.ConfigReplacer;
import com.hazelcast.config.security.StaticCredentialsFactory;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.diagnostics.EventQueuePlugin;
import com.hazelcast.internal.diagnostics.OverloadedConnectionsPlugin;
import com.hazelcast.internal.networking.nio.NioThread;
import com.hazelcast.internal.util.ICMPHelper;
import com.hazelcast.internal.util.OperatingSystemMXBeanSupport;
import com.hazelcast.internal.util.RandomPicker;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.internal.util.ThreadLocalRandomProvider;
import com.hazelcast.internal.util.executor.StripedExecutor;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.ssl.SSLContextFactory;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.query.extractor.ValueExtractor;
import com.hazelcast.security.ICredentialsFactory;
import com.hazelcast.spi.discovery.DiscoveryStrategyFactory;
import com.hazelcast.spi.discovery.NodeFilter;
import com.hazelcast.topic.MessageListener;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.deployment.builditem.ExtensionSslNativeSupportBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.HotDeploymentWatchedFileBuildItem;
import io.quarkus.deployment.builditem.nativeimage.NativeImageResourceBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;
import io.quarkus.deployment.builditem.nativeimage.RuntimeInitializedClassBuildItem;
import io.quarkus.deployment.builditem.nativeimage.RuntimeReinitializedClassBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ServiceProviderBuildItem;
import io.quarkus.hazelcast.client.runtime.HazelcastClientBytecodeRecorder;
import io.quarkus.hazelcast.client.runtime.HazelcastClientProducer;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.IndexView;

import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import static io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem.builder;

class HazelcastClientProcessor {

    /**
     * Types that users may implement and reference by class name in the Hazelcast client configuration.
     */
    private static final DotName[] USER_IMPLEMENTABLE_TYPES = Stream.of(
            AbstractLoadBalancer.class,
            ClientProxyFactory.class,
            ConfigReplacer.class,
            DataSerializable.class,
            DataSerializableFactory.class,
            EntryListener.class,
            ICredentialsFactory.class,
            ItemListener.class,
            LifecycleListener.class,
            LoadBalancer.class,
            MapListener.class,
            MembershipListener.class,
            MessageListener.class,
            MigrationListener.class,
            NodeFilter.class,
            PortableFactory.class,
            Serializer.class,
            SocketInterceptor.class,
            SSLContextFactory.class,
            ValueExtractor.class)
            .map(DotName::createSimple)
            .toArray(DotName[]::new);

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem("hazelcast-client");
    }

    @BuildStep
    void enableSSL(BuildProducer<ExtensionSslNativeSupportBuildItem> ssl) {
        ssl.produce(new ExtensionSslNativeSupportBuildItem("hazelcast-client"));
    }

    @BuildStep
    void registerServiceProviders(BuildProducer<ServiceProviderBuildItem> serviceProviders) {
        serviceProviders.produce(ServiceProviderBuildItem.allProvidersFromClassPath(DiscoveryStrategyFactory.class.getName()));
        serviceProviders.produce(ServiceProviderBuildItem.allProvidersFromClassPath(ClientExtension.class.getName()));
    }

    @BuildStep
    void setup(BuildProducer<AdditionalBeanBuildItem> additionalBeans) {
        additionalBeans.produce(AdditionalBeanBuildItem.unremovableOf(HazelcastClientProducer.class));
    }

    @BuildStep
    @Record(ExecutionTime.RUNTIME_INIT)
    HazelcastClientConfiguredBuildItem resolveClientProperties(HazelcastClientBytecodeRecorder recorder) {
        recorder.configureRuntimeProperties();
        return new HazelcastClientConfiguredBuildItem();
    }

    @BuildStep
    void registerConfigurationFiles(
            BuildProducer<NativeImageResourceBuildItem> resources,
            BuildProducer<HotDeploymentWatchedFileBuildItem> watchedFiles) {
        resources.produce(new NativeImageResourceBuildItem(
                "hazelcast-client.yml",
                "hazelcast-client-default.xml",
                "hazelcast-client.yaml",
                "hazelcast-client.xml"));

        watchedFiles.produce(new HotDeploymentWatchedFileBuildItem("hazelcast-client.yml"));
        watchedFiles.produce(new HotDeploymentWatchedFileBuildItem("hazelcast-client.yaml"));
        watchedFiles.produce(new HotDeploymentWatchedFileBuildItem("hazelcast-client.xml"));
    }

    @BuildStep
    void registerReflectivelyCreatedClasses(BuildProducer<ReflectiveClassBuildItem> reflectiveClasses) {
        // Built-in implementations that Hazelcast instantiates by class name
        reflectiveClasses.produce(builder(
                HazelcastClientCachingProvider.class,
                EncryptionReplacer.class,
                PropertyReplacer.class,
                StaticCredentialsFactory.class)
                .reason(getClass().getName())
                .build());
    }

    @BuildStep
    void registerUserImplementations(CombinedIndexBuildItem combinedIndex,
            BuildProducer<ReflectiveClassBuildItem> reflectiveClasses) {
        // Hazelcast only needs to instantiate user implementations via their no-arg constructor
        // (see com.hazelcast.internal.nio.ClassLoaderUtil#newInstance)
        IndexView index = combinedIndex.getIndex();
        Set<String> classNames = new TreeSet<>();
        for (DotName type : USER_IMPLEMENTABLE_TYPES) {
            addInstantiableClasses(index.getAllKnownImplementors(type), classNames);
            addInstantiableClasses(index.getAllKnownSubclasses(type), classNames);
        }

        if (!classNames.isEmpty()) {
            reflectiveClasses.produce(builder(classNames.toArray(new String[0]))
                    .reason(getClass().getName())
                    .build());
        }
    }

    @BuildStep
    void registerDataStructureProxies(BuildProducer<ReflectiveClassBuildItem> reflectiveClasses) {
        reflectiveClasses.produce(builder(
                // instantiated reflectively via their public constructor by com.hazelcast.client.impl.spi.ProxyManager
                ClientCardinalityEstimatorProxy.class,
                ClientDurableExecutorServiceProxy.class,
                ClientExecutorServiceProxy.class,
                ClientFlakeIdGeneratorProxy.class,
                ClientListProxy.class,
                ClientMultiMapProxy.class,
                ClientPNCounterProxy.class,
                ClientQueueProxy.class,
                ClientReplicatedMapProxy.class,
                ClientRingbufferProxy.class,
                ClientScheduledExecutorProxy.class,
                ClientSetProxy.class,
                ClientTopicProxy.class,
                XAResourceProxy.class,
                // instantiated reflectively by com.hazelcast.internal.config.ConfigUtils#getConfig
                ClientReliableTopicConfig.class,
                ClientFlakeIdGeneratorConfig.class)
                .reason(getClass().getName())
                .build());
    }

    @BuildStep
    void registerICMPHelper(BuildProducer<NativeImageResourceBuildItem> resources,
            BuildProducer<RuntimeReinitializedClassBuildItem> reinitializedClasses) {
        resources.produce(new NativeImageResourceBuildItem(
                "lib/linux-x86/libicmp_helper.so",
                "lib/linux-x86_64/libicmp_helper.so"));
        reinitializedClasses.produce(new RuntimeReinitializedClassBuildItem(ICMPHelper.class.getName()));
    }

    @BuildStep
    void initializeRandomHolderAtRuntime(BuildProducer<RuntimeInitializedClassBuildItem> runtimeInitializedClasses) {
        runtimeInitializedClasses.produce(new RuntimeInitializedClassBuildItem("com.hazelcast.client.impl.connection.tcp.WaitStrategy"));
        runtimeInitializedClasses.produce(new RuntimeInitializedClassBuildItem(TopicAddMessageListenerMessageTask.class.getName()));
        runtimeInitializedClasses.produce(new RuntimeInitializedClassBuildItem(ClientDurableExecutorServiceProxy.class.getName()));
        runtimeInitializedClasses.produce(new RuntimeInitializedClassBuildItem(ClientExecutorServiceProxy.class.getName()));
        runtimeInitializedClasses.produce(new RuntimeInitializedClassBuildItem(RandomLB.class.getName()));
        runtimeInitializedClasses.produce(new RuntimeInitializedClassBuildItem(EventQueuePlugin.class.getName()));
        runtimeInitializedClasses.produce(new RuntimeInitializedClassBuildItem(OverloadedConnectionsPlugin.class.getName()));
        runtimeInitializedClasses.produce(new RuntimeInitializedClassBuildItem(NioThread.class.getName()));
        runtimeInitializedClasses.produce(new RuntimeInitializedClassBuildItem(StripedExecutor.class.getName()));
        runtimeInitializedClasses.produce(new RuntimeInitializedClassBuildItem(ThreadLocalRandomProvider.class.getName()));
        runtimeInitializedClasses.produce(new RuntimeInitializedClassBuildItem(RandomPicker.class.getName()));
        runtimeInitializedClasses.produce(new RuntimeInitializedClassBuildItem(OperatingSystemMXBeanSupport.class.getName()));
    }

    @BuildStep
    void registerXMLParsingUtilities(BuildProducer<NativeImageResourceBuildItem> resources) {
        // Hazelcast only validates against the XSD matching its own release version
        // (see com.hazelcast.config.AbstractXmlConfigHelper#getReleaseVersion)
        String[] version = StringUtil.tokenizeVersionString(BuildInfoProvider.getBuildInfo().getVersion());
        resources.produce(new NativeImageResourceBuildItem(
                "hazelcast-client-config-" + version[0] + "." + version[1] + ".xsd"));
    }

    private static void addInstantiableClasses(Collection<ClassInfo> candidates, Set<String> classNames) {
        for (ClassInfo candidate : candidates) {
            if (!candidate.isInterface() && !Modifier.isAbstract(candidate.flags())) {
                classNames.add(candidate.name().toString());
            }
        }
    }
}
