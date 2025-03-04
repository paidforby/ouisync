import 'client.dart' show Subscription;
import 'ouisync_plugin.dart' show Session;

export 'client.dart' show Subscription;

// Version is incremented every time the monitor or any of it's values or
// children changes.
typedef Version = int;

// Used to identify child state monitors.
class MonitorId implements Comparable<MonitorId> {
  final String _name;
  // This one is now shown to the user, it allows us to have multiple monitors of the same name.
  final int _disambiguator;

  String get name => _name;

  MonitorId(this._name, this._disambiguator);

  // For when we expect the name to uniquely identify the child.
  static MonitorId expectUnique(String name) => MonitorId(name, 0);

  static MonitorId parse(String raw) {
    // A string in the format "name:disambiguator".
    final colon = raw.lastIndexOf(':');
    final name = raw.substring(0, colon);
    final disambiguator = int.parse(raw.substring(colon + 1));

    return MonitorId(name, disambiguator);
  }

  @override
  String toString() {
    return "$_name:$_disambiguator";
  }

  @override
  int compareTo(MonitorId other) {
    // Negative return value means `this` will be appear first.
    final cmp = _name.compareTo(other._name);
    if (cmp == 0) {
      return _disambiguator - other._disambiguator;
    }
    return cmp;
  }
}

class StateMonitorNode {
  final List<MonitorId> path;
  final Map<String, String> values;
  final List<MonitorId> children;

  StateMonitorNode(
    this.path,
    this.values,
    this.children,
  );

  static StateMonitorNode _decode(
    List<MonitorId> path,
    List<Object?> raw,
  ) {
    final values = _decodeValues(raw[0]);
    final children = _decodeChildren(raw[1]);

    return StateMonitorNode(
      path,
      values,
      children,
    );
  }

  static Map<String, String> _decodeValues(Object? raw) {
    final rawMap = raw as Map<Object?, Object?>;
    return rawMap.cast<String, String>();
  }

  static List<MonitorId> _decodeChildren(Object? raw) {
    final rawList = raw as List<Object?>;
    return rawList.cast<String>().map((id) => MonitorId.parse(id)).toList();
  }

  int? parseIntValue(String name) {
    final str = values[name];
    if (str == null) return null;
    return int.tryParse(str);
  }

  @override
  String toString() =>
      "StateMonitorNode { values:$values, children:$children }";
}

class StateMonitor {
  final Session session;
  List<MonitorId> path;

  StateMonitor._(this.session, this.path);

  static StateMonitor getRoot(Session session) =>
      StateMonitor._(session, <MonitorId>[]);

  StateMonitor child(MonitorId childId) =>
      StateMonitor._(session, [...path, childId]);

  Subscription subscribe() => Subscription(
      session.client, "state_monitor", path.map((id) => id.toString()));

  @override
  String toString() => "StateMonitor($path)";

  Future<StateMonitorNode?> load() async {
    try {
      final list = await session.client
              .invoke("state_monitor_get", path.map((id) => id.toString()))
          as List<Object?>;
      return StateMonitorNode._decode(path, list);
    } catch (e) {
      print('failed to load state monitor node at $path: $e');
      return null;
    }
  }
}
