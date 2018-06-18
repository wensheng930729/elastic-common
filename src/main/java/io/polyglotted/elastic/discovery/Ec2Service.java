package io.polyglotted.elastic.discovery;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import static org.elasticsearch.common.settings.Setting.Property.NodeScope;

interface Ec2Service {
    class HostType {
        static final String PRIVATE_IP = "private_ip";
        static final String PUBLIC_IP = "public_ip";
        static final String PRIVATE_DNS = "private_dns";
        static final String PUBLIC_DNS = "public_dns";
        static final String TAG_PREFIX = "tag:";
    }

    Setting<String> REGION_SETTING = new Setting<>("cloud.aws.region", "", s -> s.toLowerCase(Locale.ROOT), NodeScope);
    Setting<SecureString> ACCESS_KEY_SETTING = SecureSetting.secureString("discovery.ec2.access_key", null);
    Setting<SecureString> SECRET_KEY_SETTING = SecureSetting.secureString("discovery.ec2.secret_key", null);
    Setting<String> ENDPOINT_SETTING = new Setting<>("discovery.ec2.endpoint", "", s -> s.toLowerCase(Locale.ROOT), NodeScope);
    Setting<Protocol> PROTOCOL_SETTING = new Setting<>("discovery.ec2.protocol", "https",
        s -> Protocol.valueOf(s.toUpperCase(Locale.ROOT)), NodeScope);
    Setting<String> PROXY_HOST_SETTING = Setting.simpleString("discovery.ec2.proxy.host", NodeScope);
    Setting<Integer> PROXY_PORT_SETTING = Setting.intSetting("discovery.ec2.proxy.port", 80, 0, 1 << 16, NodeScope);
    Setting<SecureString> PROXY_USERNAME_SETTING = SecureSetting.secureString("discovery.ec2.proxy.username", null);
    Setting<SecureString> PROXY_PASSWORD_SETTING = SecureSetting.secureString("discovery.ec2.proxy.password", null);
    Setting<TimeValue> READ_TIMEOUT_SETTING = Setting.timeSetting("discovery.ec2.read_timeout",
        TimeValue.timeValueMillis(ClientConfiguration.DEFAULT_SOCKET_TIMEOUT), NodeScope);

    Setting<String> HOST_TYPE_SETTING = new Setting<>("discovery.ec2.host_type", HostType.PRIVATE_IP, Function.identity(), NodeScope);
    Setting<Boolean> ANY_GROUP_SETTING = Setting.boolSetting("discovery.ec2.any_group", true, NodeScope);
    Setting<List<String>> GROUPS_SETTING = Setting.listSetting("discovery.ec2.groups", new ArrayList<>(), String::toString, NodeScope);
    Setting<List<String>> AVAILABILITY_ZONES_SETTING = Setting.listSetting("discovery.ec2.availability_zones",
        Collections.emptyList(), String::toString, NodeScope);

    Setting.AffixSetting<List<String>> TAG_SETTING = Setting.prefixKeySetting("discovery.ec2.tag.",
        key -> Setting.listSetting(key, Collections.emptyList(), Function.identity(), NodeScope));
}