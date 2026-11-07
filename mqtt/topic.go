package mqtt

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// maxTopicLength is what MQTT allows an encoded topic name, which carries a two-byte length.
const maxTopicLength = 65535

// reasonTopicLimit bounds a refused topic quoted back in a rejection reason, since the topic came
// from a stranger and the reason ends up in a log.
const reasonTopicLimit = 64

// isSafeTopicName reports whether a topic is one this peer may publish an answer to.
//
// These are the publishable-topic rules from docs/mqtt5-frame-spec.md: a wildcard is a
// subscription filter rather than a destination, a leading `$` names broker-reserved space, and a
// control character has no business in a topic that will be read back out of a log or a console.
func isSafeTopicName(topic string) bool {
	if topic == "" || len(topic) > maxTopicLength {
		return false
	}
	if strings.HasPrefix(topic, "$") {
		return false
	}
	if !utf8.ValidString(topic) {
		return false
	}
	for _, character := range topic {
		if character == '+' || character == '#' || unicode.IsControl(character) {
			return false
		}
	}
	return true
}

// maxTopicSegmentLength bounds one level of a topic, which is what a peer name becomes.
const maxTopicSegmentLength = 128

// isSafeTopicSegment reports whether a value may be used as one level of a topic.
//
// A peer name arrives from the network, is interpolated into the topics this peer publishes to, and
// is used as a map key — so it must not be able to change the shape of either. A peer calling itself
// `#` would otherwise have its answers published to a wildcard the broker fans out to every
// subscriber, and one carrying a NUL could forge a subscription key belonging to somebody else,
// since NUL is what separates the parts of those keys.
//
// Deliberately the same rule as the TypeScript transport's isSafeTopicSegment, no stricter: a name
// one implementation accepts and the other refuses is an interop failure that would only show up
// against a peer somebody else wrote.
func isSafeTopicSegment(value string) bool {
	if value == "" || len(value) > maxTopicSegmentLength {
		return false
	}
	if !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if character == '+' || character == '#' || character == '/' || unicode.IsControl(character) {
			return false
		}
	}
	return true
}

// refuseResponseTopic gives the reason a request's response topic may not be answered on, or an
// empty string when it may.
//
// The topic is chosen by whoever sent the request and this peer is the one that would publish to
// it, so without a rule a caller could have this server publish anywhere its own broker
// credentials reach — into `$SYS`, over another peer's retained presence, or onto a request topic
// a third peer serves. Signing does not help here: the response topic is inside the signed
// canonical form, so a properly signed request is just as able to name a hostile one.
//
// The default boundary is the transport's own prefix, because that is where an operator already
// draws broker ACLs. AllowResponseTopic replaces that rule where an installation needs something
// else.
func (t *Transport) refuseResponseTopic(topic string, source string) string {
	if !isSafeTopicName(topic) {
		return fmt.Sprintf("unusable response topic %q", truncateForReason(topic))
	}
	if t.options.AllowResponseTopic != nil {
		if t.options.AllowResponseTopic(topic, source) {
			return ""
		}
		return fmt.Sprintf("response topic %q is not allowed here", truncateForReason(topic))
	}
	if strings.HasPrefix(topic, t.options.Prefix+"/") {
		return ""
	}
	return fmt.Sprintf("response topic %q is outside %q", truncateForReason(topic), t.options.Prefix+"/")
}

// resolveResponseTopic settles where a request's answer goes, or gives the reason it may not be
// answered at all.
//
// A request naming a topic outside the rule is refused rather than quietly answered on a derived
// topic: a caller waiting on the topic it named is not helped by a reply sent elsewhere, and the
// refusal is the signal worth surfacing. A request that names no topic is answered on the channel
// derived from its source, which is this peer's own prefix and needs no further checking.
func (t *Transport) resolveResponseTopic(responseTopic string, source string) (string, string) {
	if responseTopic == "" {
		// The derived topic is only as safe as the name it interpolates, and that name came off the
		// wire: a source of `#` would otherwise turn this peer's answer into a broadcast.
		if !isSafeTopicSegment(source) {
			return "", "no response topic, and " + MRSource + " cannot be used to derive one"
		}
		return Topic(t.options.Prefix, ChannelResponse, source), ""
	}
	if reason := t.refuseResponseTopic(responseTopic, source); reason != "" {
		return "", reason
	}
	return responseTopic, ""
}

func truncateForReason(topic string) string {
	if len(topic) <= reasonTopicLimit {
		return topic
	}
	return topic[:reasonTopicLimit] + "…"
}
