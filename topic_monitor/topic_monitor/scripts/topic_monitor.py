# Copyright 2016 Open Source Robotics Foundation, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import functools
import re
from threading import Lock, Thread
import time

import rclpy
import rclpy.logging
from rclpy.qos import QoSProfile
from rclpy.qos import QoSReliabilityPolicy

from std_msgs.msg import Float32, Header

QOS_DEPTH = 10
logger = rclpy.logging.get_logger('topic_monitor')


class MonitoredTopic:
    """Monitor for the statistics and status of a single topic."""

    def __init__(self, topic_id, stale_time, lock):
        """"Initializes a new instance of the Topic class with the given topic ID, stale time, and lock."
        Parameters:
            - topic_id (str): The unique identifier for the topic.
            - stale_time (int): The maximum amount of time in seconds before the topic's data is considered stale.
            - lock (threading.Lock): A lock used to synchronize access to the topic's data.
        Returns:
            - None: This function does not return any value.
        Processing Logic:
            - Initializes the expected value, expected value timer, initial value, received values, reception rate over time, status, status changed, and time of last data attributes.
            - Sets the expected value and initial value to None.
            - Sets the status to 'Offline'.
            - Sets the status changed flag to False.
            - Sets the time of last data to None.
            - Stores the given topic ID and stale time.
            - Stores the given lock for synchronization."""
        
        self.expected_value = None
        self.expected_value_timer = None
        self.initial_value = None
        self.lock = lock
        self.received_values = []
        self.reception_rate_over_time = []
        self.stale_time = stale_time
        self.status = 'Offline'
        self.status_changed = False
        self.time_of_last_data = None
        self.topic_id = topic_id

    def increment_expected_value(self):
        """Increments the expected value by 1.
        Parameters:
            - self (object): The object itself.
        Returns:
            - None: Does not return anything.
        Processing Logic:
            - Locks the object to prevent race conditions.
            - Checks if the expected value is not None.
            - Increments the expected value by 1."""
        
        with self.lock:
            if self.expected_value is not None:
                self.expected_value += 1

    def allowed_latency_timer_callback(self):
        """This function cancels the allowed latency timer and resets the expected value timer.
        Parameters:
            - self (class): The class instance that the function is being called on.
        Returns:
            - None: This function does not return any value.
        Processing Logic:
            - Cancels allowed latency timer.
            - Resets expected value timer."""
        
        self.allowed_latency_timer.cancel()
        self.expected_value_timer.reset()

    def get_data_from_msg(self, msg):
        """"Extracts data from a message and returns an integer value.
        Parameters:
            - msg (object): A message object containing data.
        Returns:
            - int: An integer value extracted from the message.
        Processing Logic:
            - Extracts data from message frame_id.
            - Finds the first underscore in the data.
            - If underscore is found, returns data before underscore.
            - If underscore is not found, returns entire data.
            - Converts data to integer if possible.
            - Returns 0 if data is empty.""""
        
        data = msg.frame_id
        idx = data.find('_')
        data = data[:idx] if idx != -1 else data
        return int(data) if data else 0

    def topic_data_callback(self, msg, logger_=logger):
        """This function handles the received data from a topic and updates the expected value and status accordingly.
        Parameters:
            - msg (type): A message containing the received data.
            - logger_ (type): (Optional) A logger object to log information.
        Returns:
            - status (str): The current status of the topic.
        Processing Logic:
            - Update expected value if first value from topic.
            - Update status if topic was previously offline.
            - Update expected value and reset timer.
            - Update time of last data.
            - Check if status has changed."""
        
        received_value = self.get_data_from_msg(msg)
        logger_.info('%s: %s' % (self.topic_id, str(received_value)))
        status = 'Alive'
        with self.lock:
            if self.expected_value is None:
                # This is the first value from the topic
                self.expected_value = received_value
                self.initial_value = received_value
                self.allowed_latency_timer.reset()
            if received_value == -1:
                # The topic was previously offline
                status = 'Offline'
                self.expected_value_timer.cancel()
                self.expected_value = None
            else:
                self.expected_value_timer.cancel()
                self.received_values.append(received_value)
                self.expected_value = received_value + 1
                self.allowed_latency_timer.reset()
            self.time_of_last_data = time.time()  # TODO(dhood): time stamp of msg
            status_changed = status != self.status
            self.status_changed |= status_changed  # don't clear the flag before check_status
            self.status = status

    def check_status(self, current_time=time.time()):
        """Checks the status of a topic and returns whether the status has changed.
        Parameters:
            - current_time (float): The current time in seconds since the epoch. Defaults to the current time.
        Returns:
            - status_changed (bool): True if the status has changed, False otherwise.
        Processing Logic:
            - Checks if the topic has gone offline or come back online.
            - Checks if the topic has gone stale.
            - Sets the status to 'Stale' if it has gone stale.
            - Resets the status_changed flag to False.
        Example:
            status_changed = check_status(current_time=1609459200.0)
            # status_changed = True"""
        
        # A status could have changed if a topic goes offline or comes back online
        status_changed = self.status_changed

        # Additionally we check if it has gone stale:
        if self.status != 'Offline':
            elapsed_time = current_time - self.time_of_last_data
            if elapsed_time > self.stale_time:
                status_changed |= self.status != 'Stale'
                self.status = 'Stale'

        self.status_changed = False
        return status_changed

    def current_reception_rate(self, window_size):
        """Calculates the current reception rate of a device.
        Parameters:
            - window_size (int): The size of the window in which the expected values are checked.
        Returns:
            - rate (float): The current reception rate as a decimal value.
        Processing Logic:
            - Checks if the device is currently offline.
            - Calculates the expected values within the window.
            - Counts how many of the expected values have been received.
            - Calculates the reception rate by dividing the count by the total expected values.
        Example:
            device = Device()
            device.current_reception_rate(10)
            # Returns 0.75 if 3 out of 4 expected values were received within the window of size 10."""
        
        rate = None
        if self.status != 'Offline':
            expected_values = range(
                max(self.initial_value, self.expected_value - window_size),
                self.expected_value)
            # How many of the expected values have been received?
            count = len(set(expected_values) & set(self.received_values))
            rate = count / len(expected_values)
        return rate


class TopicMonitor:
    """Monitor of a set of topics that match a specified topic name pattern."""

    def __init__(self, window_size):
        """Initializes the object with the given window size.
        Parameters:
            - window_size (int): The size of the window to be used for data processing.
        Returns:
            - None: This function does not return anything.
        Processing Logic:
            - Compile regular expression pattern.
            - Initialize empty dictionaries and a lock.
            - Set the name of the reception rate topic.
            - Set the status changed flag to False.
            - Store the given window size."""
        
        self.data_topic_pattern = re.compile(r'(/(?P<data_name>\w*)_data_?(?P<reliability>\w*))')
        self.monitored_topics = {}
        self.monitored_topics_lock = Lock()
        self.publishers = {}
        self.reception_rate_topic_name = 'reception_rate'
        self.status_changed = False
        self.window_size = window_size

    def add_monitored_topic(
            self, topic_type, topic_name, node, qos_profile,
            expected_period=1.0, allowed_latency=1.0, stale_time=1.0):
        """This function adds a subscription to a given topic and creates a timer for maintaining the expected value received on the topic. It also creates a publisher for the reception rate of the topic.
        Parameters:
            - topic_type (str): The type of the topic to be subscribed to.
            - topic_name (str): The name of the topic to be subscribed to.
            - node (Node): The node that will be used to create the subscription, timer, and publisher.
            - qos_profile (QoSProfile): The quality of service profile to be used for the subscription.
            - expected_period (float): The time interval for maintaining the expected value received on the topic. Default value is 1.0.
            - allowed_latency (float): The time interval for the allowed latency before starting the expected value timer. Default value is 1.0.
            - stale_time (float): The time interval for determining if the topic data is stale. Default value is 1.0.
        Returns:
            - None: This function does not return any value.
        Processing Logic:
            - Creates a MonitoredTopic object for the given topic name and stale time.
            - Subscribes to the topic using the given node and qos profile.
            - Creates a timer for maintaining the expected value received on the topic.
            - Creates a one-shot timer for the allowed latency before starting the expected value timer.
            - Creates a publisher for the reception rate of the topic.
            - Adds the expected value timer, allowed latency timer, publisher, and monitored topic to their respective lists/objects."""
        
        # Create a subscription to the topic
        monitored_topic = MonitoredTopic(topic_name, stale_time, lock=self.monitored_topics_lock)
        node_logger = node.get_logger()
        node_logger.info('Subscribing to topic: %s' % topic_name)
        sub = node.create_subscription(
            topic_type,
            topic_name,
            functools.partial(monitored_topic.topic_data_callback, logger_=node_logger),
            qos_profile)
        assert sub  # prevent unused warning

        # Create a timer for maintaining the expected value received on the topic
        expected_value_timer = node.create_timer(
            expected_period, monitored_topic.increment_expected_value)
        expected_value_timer.cancel()

        # Create a one-shot timer that won't start the expected value timer until the allowed
        # latency has elapsed
        allowed_latency_timer = node.create_timer(
            allowed_latency, monitored_topic.allowed_latency_timer_callback)
        allowed_latency_timer.cancel()

        # Create a publisher for the reception rate of the topic
        reception_rate_topic_name = self.reception_rate_topic_name + topic_name

        # TODO(dhood): remove this workaround
        # once https://github.com/ros2/rmw_connext/issues/234 is resolved
        reception_rate_topic_name += '_'

        node.get_logger().info(
            'Publishing reception rate on topic: %s' % reception_rate_topic_name)
        reception_rate_publisher = node.create_publisher(
            Float32, reception_rate_topic_name, 10)

        with self.monitored_topics_lock:
            monitored_topic.expected_value_timer = expected_value_timer
            monitored_topic.allowed_latency_timer = allowed_latency_timer
            self.publishers[topic_name] = reception_rate_publisher
            self.monitored_topics[topic_name] = monitored_topic

    def is_supported_type(self, type_name):
        """This function checks if the given type name is supported.
        Parameters:
            - type_name (str): The name of the type to be checked.
        Returns:
            - bool: True if the type is supported, False otherwise.
        Processing Logic:
            - Checks if the given type name matches 'std_msgs/msg/Header'.
            - Returns True if it matches, False otherwise."""
        
        return type_name == 'std_msgs/msg/Header'

    def get_topic_info(self, topic_name):
        """Infer topic info (e.g. QoS reliability) from the topic name."""
        match = re.search(self.data_topic_pattern, topic_name)
        if match and match.groups():
            if match.groups()[0] != topic_name:
                # Only part of the topic name matches
                return None

            topic_info = {'reliability': 'reliable'}
            topic_info['topic_name'] = topic_name
            topic_info['data_name'] = match.group('data_name')

            reliability = match.group('reliability')
            if reliability == 'best_effort':
                topic_info['reliability'] = 'best_effort'
            return topic_info

    def update_topic_statuses(self):
        """Updates the status of all monitored topics and returns whether any status has changed.
        Parameters:
            - self (class): The class instance.
        Returns:
            - any_status_changed (bool): True if any status has changed, False otherwise.
        Processing Logic:
            - Update status of all topics.
            - Check if any status has changed.
            - Return whether any status has changed."""
        
        any_status_changed = False
        current_time = time.time()
        with self.monitored_topics_lock:
            for topic_id, monitored_topic in self.monitored_topics.items():
                status_changed = monitored_topic.check_status(current_time)
                any_status_changed |= status_changed
        return any_status_changed

    def output_status(self):
        """Outputs the status of the monitored topics.
        Parameters:
            - self (class): The class object.
        Returns:
            - None: No return value.
        Processing Logic:
            - Logs the status of each monitored topic.
            - Uses the logger.info() function.
            - Acquires the monitored_topics_lock.
            - Iterates through the monitored_topics dictionary.
            - Logs the topic_id and the status of the monitored_topic.
            - Uses the string formatting operator (%).
            - Logs a line of dashes.
            - Uses the logger.info() function."""
        
        logger.info('---------------')
        with self.monitored_topics_lock:
            for topic_id, monitored_topic in self.monitored_topics.items():
                logger.info('%s: %s' % (topic_id, monitored_topic.status))
        logger.info('---------------')

    def check_status(self):
        """"Checks the status of a topic and outputs the status if it has changed.
        Parameters:
            - self (object): The object containing the topic to be checked.
        Returns:
            - status_changed (bool): True if the status has changed, False otherwise.
        Processing Logic:
            - Checks if the status has changed.
            - Updates the topic statuses.
            - Outputs the status if it has changed.
            - Returns a boolean indicating if the status has changed.
        Example:
            check_status(topic) # Returns True if the status has changed and outputs the new status.""""
        
        if status_changed := self.update_topic_statuses():
            self.output_status()
        return status_changed

    def calculate_statistics(self):
        """Calculates the reception rate for monitored topics and publishes it.
        Parameters:
            - self (object): The object calling the function.
        Returns:
            - None: The function does not return anything.
        Processing Logic:
            - Calculate reception rate for each topic.
            - Append rate to reception rate over time.
            - Publish the rate for each topic."""
        
        with self.monitored_topics_lock:
            for topic_id, monitored_topic in self.monitored_topics.items():
                rate = monitored_topic.current_reception_rate(self.window_size)
                monitored_topic.reception_rate_over_time.append(rate)
                rateMsg = Float32()
                rateMsg.data = rate if rate is not None else 0.0
                self.publishers[topic_id].publish(rateMsg)

    def get_window_size(self):
        """"Returns the window size of the object.
        Parameters:
            - self (object): The object whose window size is being retrieved.
        Returns:
            - int: The window size of the object.
        Processing Logic:
            - Retrieve the window size attribute.
            - Return the window size.
        """"
        
        return self.window_size


class TopicMonitorDisplay:
    """Display of the monitored topic reception rates."""

    def __init__(self, topic_monitor, update_period):
        """"""
        
        self.colors = 'bgrcmykw'
        self.markers = 'o>sp*hDx+'
        self.monitored_topics = []
        self.reception_rate_plots = {}
        self.start_time = time.time()
        self.topic_count = 0
        self.topic_monitor = topic_monitor
        self.x_data = []
        self.x_range = 120  # points
        self.x_range_s = self.x_range * update_period  # seconds

        self.make_plot()

    def make_plot(self):
        """"""
        
        self.fig = plt.figure()
        plt.title('Reception rate over time')
        plt.xlabel('Time (s)')
        plt.ylabel('Reception rate (last %i msgs)' % self.topic_monitor.get_window_size())
        self.ax = self.fig.get_axes()[0]
        self.ax.axis([0, self.x_range_s, 0, 1.1])

        # Shrink axis' height to make room for legend
        shrink_amnt = 0.2
        box = self.ax.get_position()
        self.ax.set_position(
            [box.x0, box.y0 + box.height * shrink_amnt, box.width, box.height * (1 - shrink_amnt)])

    def add_monitored_topic(self, topic_name):
        """"""
        
        # Make first instance of the line so that we only have to update it later
        line, = self.ax.plot(
            [], [], '-', color=self.colors[self.topic_count % len(self.colors)],
            marker=self.markers[self.topic_count % len(self.markers)], label=topic_name)
        self.reception_rate_plots[topic_name] = line

        # Update the plot legend to include the new line
        self.ax.legend(
            loc='upper center', bbox_to_anchor=(0.5, -0.1), fancybox=True, shadow=True, ncol=2)

        self.topic_count += 1
        self.monitored_topics.append(topic_name)

    def update_display(self):
        """"""
        
        now = time.time()
        now_relative = now - self.start_time
        self.x_data.append(now_relative)
        with self.topic_monitor.monitored_topics_lock:
            for topic_name, monitored_topic in self.topic_monitor.monitored_topics.items():
                if topic_name not in self.monitored_topics:
                    self.add_monitored_topic(topic_name)

                y_data = monitored_topic.reception_rate_over_time
                line = self.reception_rate_plots[topic_name]
                line.set_ydata(y_data)
                line.set_xdata(self.x_data[-len(y_data):])

                # Make the line slightly transparent if the topic is stale
                line.set_alpha(0.5 if monitored_topic.status == 'Stale' else 1.0)

        self.ax.axis(
            [now_relative - self.x_range_s, now_relative, 0, 1.1])

        self.fig.canvas.draw()
        plt.pause(0.0001)
        plt.show(block=False)


class DataReceivingThread(Thread):

    def __init__(self, topic_monitor, options):
        """"""
        
        super(DataReceivingThread, self).__init__()
        rclpy.init()
        self.topic_monitor = topic_monitor
        self.options = options

    def run(self):
        self.node = rclpy.create_node('topic_monitor')
        try:
            run_topic_listening(self.node, self.topic_monitor, self.options)
        except KeyboardInterrupt:
            self.stop()
            raise

    def stop(self):
        self.node.destroy_node()
        rclpy.shutdown()


def run_topic_listening(node, topic_monitor, options):
    """Subscribe to relevant topics and manage the data received from susbcriptions."""
    already_ignored_topics = set()
    while rclpy.ok():
        # Check if there is a new topic online
        # TODO(dhood): use graph events rather than polling
        topic_names_and_types = node.get_topic_names_and_types()

        for topic_name, type_names in topic_names_and_types:
            # Infer the appropriate QoS profile from the topic name
            if (topic_info := topic_monitor.get_topic_info(topic_name)) is None:
                # The topic is not for being monitored
                continue

            if len(type_names) != 1:
                if topic_name not in already_ignored_topics:
                    node.get_logger().info(
                        "Warning: ignoring topic '%s', which has more than one type: [%s]"
                        % (topic_name, ', '.join(type_names)))
                    already_ignored_topics.add(topic_name)
                continue

            type_name = type_names[0]
            if not topic_monitor.is_supported_type(type_name):
                if topic_name not in already_ignored_topics:
                    node.get_logger().info(
                        "Warning: ignoring topic '%s' because its message type (%s)"
                        'is not supported.'
                        % (topic_name, type_name))
                    already_ignored_topics.add(topic_name)
                continue

            if is_new_topic := topic_name and topic_name not in topic_monitor.monitored_topics:
                # Register new topic with the monitor
                qos_profile = QoSProfile(depth=10)
                qos_profile.depth = QOS_DEPTH
                if topic_info['reliability'] == 'best_effort':
                    qos_profile.reliability = \
                        QoSReliabilityPolicy.BEST_EFFORT
                topic_monitor.add_monitored_topic(
                    Header, topic_name, node, qos_profile,
                    options.expected_period, options.allowed_latency, options.stale_time)

        # Wait for messages with a timeout, otherwise this thread will block any other threads
        # until a message is received
        rclpy.spin_once(node, timeout_sec=0.05)


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '-d', '--display', dest='show_display', action='store_true', default=False,
        help='Display the reception rate of topics (requires matplotlib)')

    parser.add_argument(
        '-t', '--expected-period', type=float, nargs='?', default=0.5,
        help='Expected time in seconds between received messages on a topic')

    parser.add_argument(
        '-s', '--stale-time', type=float, nargs='?', default=1.0,
        help='Time in seconds without receiving messages before a topic is considered stale')

    parser.add_argument(
        '-l', '--allowed-latency', type=float, nargs='?', default=1.0,
        help='Allowed latency in seconds between receiving expected messages')

    parser.add_argument(
        '-c', '--stats-calc-period', type=float, nargs='?', default=1.0,
        help='Time in seconds between calculating topic statistics')

    parser.add_argument(
        '-n', '--window-size', type=int, nargs='?', default=20,
        help='Number of messages in calculation of topic statistics')

    args = parser.parse_args()
    if args.show_display:
        try:
            global plt
            import matplotlib.pyplot as plt
        except ImportError:
            raise RuntimeError('The --display option requires matplotlib to be installed')

    topic_monitor = TopicMonitor(args.window_size)

    try:
        # Run two infinite loops simultaneously: one for receiving data (subscribing to topics and
        # handling callbacks), and another for processing the received data (calculating the
        # reception rate and publishing/displaying it).

        # Since the display needs to happen in the main thread, we run the "data processing" loop
        # in the main thread and run the "data receiving" loop in a secondary thread.

        # Start the "data receiving" loop in a new thread
        data_receiving_thread = DataReceivingThread(topic_monitor, args)
        data_receiving_thread.start()

        # Start the "data processing" loop in the main thread
        # Process the data that has been received from topic subscriptions
        if args.show_display:
            topic_monitor_display = TopicMonitorDisplay(topic_monitor, args.stats_calc_period)

        last_time = time.time()
        while data_receiving_thread.is_alive():
            now = time.time()
            if now - last_time > args.stats_calc_period:
                last_time = now
                topic_monitor.check_status()
                topic_monitor.calculate_statistics()
                if args.show_display:
                    topic_monitor_display.update_display()
                # sleep the main thread so background threads can do work
                time_to_sleep = args.stats_calc_period - (time.time() - now)
                if time_to_sleep > 0:
                    time.sleep(time_to_sleep)

    finally:
        if data_receiving_thread.is_alive():
            data_receiving_thread.stop()
        # Block this thread until the other thread terminates
        data_receiving_thread.join()


if __name__ == '__main__':
    main()
