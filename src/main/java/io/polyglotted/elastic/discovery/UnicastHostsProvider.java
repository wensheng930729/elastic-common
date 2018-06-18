package io.polyglotted.elastic.discovery;

import com.amazonaws.AmazonClientException;
import com.amazonaws.http.IdleConnectionReaper;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.GroupIdentifier;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.Tag;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.common.settings.Settings;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.disjoint;

@Slf4j
public final class UnicastHostsProvider implements Closeable {
    private final AmazonEC2 client;
    private final boolean bindAnyGroup;
    private final Set<String> groups;
    private final Map<String, List<String>> tags;
    private final Set<String> availabilityZones;
    private final String hostType;

    public static List<String> fetchEc2Addresses(Settings settings) throws IOException {
        try (UnicastHostsProvider provider = new UnicastHostsProvider(settings)) { return provider.fetchAddresses(); }
    }

    private UnicastHostsProvider(Settings settings) {
        this.client = Ec2ServiceImpl.client(settings);
        this.hostType = Ec2Service.HOST_TYPE_SETTING.get(settings);
        this.bindAnyGroup = Ec2Service.ANY_GROUP_SETTING.get(settings);
        this.groups = new HashSet<>();
        this.groups.addAll(Ec2Service.GROUPS_SETTING.get(settings));
        this.tags = Ec2Service.TAG_SETTING.getAsMap(settings);
        this.availabilityZones = new HashSet<>();
        availabilityZones.addAll(Ec2Service.AVAILABILITY_ZONES_SETTING.get(settings));
        if (log.isDebugEnabled()) {
            log.debug("using host_type [{}], tags [{}], groups [{}] with any_group [{}], availability_zones [{}]",
                hostType, tags, groups, bindAnyGroup, availabilityZones);
        }
    }

    @Override
    public void close() throws IOException {
        if (client != null) { client.shutdown(); }
        IdleConnectionReaper.shutdown();
    }

    private List<String> fetchAddresses() {
        List<String> addresses = new ArrayList<>();

        DescribeInstancesResult descInstances;
        try {
            descInstances = client.describeInstances(buildDescribeInstancesRequest());
        } catch (AmazonClientException e) {
            log.info("Exception while retrieving instance list from AWS API: {}", e.getMessage());
            log.debug("Full exception:", e);
            return addresses;
        }

        log.trace("building dynamic unicast discovery nodes...");
        for (Reservation reservation : descInstances.getReservations()) {
            for (Instance instance : reservation.getInstances()) {
                if (!groups.isEmpty()) {
                    List<GroupIdentifier> instanceSecurityGroups = instance.getSecurityGroups();
                    List<String> securityGroupNames = new ArrayList<>(instanceSecurityGroups.size());
                    List<String> securityGroupIds = new ArrayList<>(instanceSecurityGroups.size());
                    for (GroupIdentifier sg : instanceSecurityGroups) {
                        securityGroupNames.add(sg.getGroupName());
                        securityGroupIds.add(sg.getGroupId());
                    }
                    if (bindAnyGroup) {
                        if (disjoint(securityGroupNames, groups) && disjoint(securityGroupIds, groups)) {
                            log.trace("filtering out instance {} based on groups {}, not part of {}", instance.getInstanceId(),
                                instanceSecurityGroups, groups);
                            continue;
                        }
                    }
                    else {
                        if (!(securityGroupNames.containsAll(groups) || securityGroupIds.containsAll(groups))) {
                            log.trace("filtering out instance {} based on groups {}, does not include all of {}",
                                instance.getInstanceId(), instanceSecurityGroups, groups);
                            continue;
                        }
                    }
                }

                String address = null;
                if (hostType.equals(Ec2Service.HostType.PRIVATE_DNS)) { address = instance.getPrivateDnsName(); }
                else if (hostType.equals(Ec2Service.HostType.PRIVATE_IP)) { address = instance.getPrivateIpAddress(); }
                else if (hostType.equals(Ec2Service.HostType.PUBLIC_DNS)) { address = instance.getPublicDnsName(); }
                else if (hostType.equals(Ec2Service.HostType.PUBLIC_IP)) { address = instance.getPublicIpAddress(); }
                else if (hostType.startsWith(Ec2Service.HostType.TAG_PREFIX)) {
                    String tagName = hostType.substring(Ec2Service.HostType.TAG_PREFIX.length());
                    log.debug("reading hostname from [{}] instance tag", tagName);
                    List<Tag> tags = instance.getTags();
                    for (Tag tag : tags) {
                        if (tag.getKey().equals(tagName)) { address = tag.getValue(); log.debug("using [{}] as the instance address", address); }
                    }
                }
                else {
                    throw new IllegalArgumentException(hostType + " is unknown for discovery.ec2.host_type");
                }
                if (address != null) { addresses.add(address); }
            }
        }
        log.debug("using dynamic discovery addresses {}", addresses);
        return addresses;
    }

    private DescribeInstancesRequest buildDescribeInstancesRequest() {
        DescribeInstancesRequest describeInstancesRequest = new DescribeInstancesRequest()
            .withFilters(new Filter("instance-state-name").withValues("running", "pending"));
        for (Map.Entry<String, List<String>> tagFilter : tags.entrySet()) {
            describeInstancesRequest.withFilters(new Filter("tag:" + tagFilter.getKey()).withValues(tagFilter.getValue()));
        }
        if (!availabilityZones.isEmpty()) {
            describeInstancesRequest.withFilters(new Filter("availability-zone").withValues(availabilityZones));
        }
        return describeInstancesRequest;
    }
}