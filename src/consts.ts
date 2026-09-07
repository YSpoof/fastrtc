// RFC 8841 default max-message-size when SCTP has not negotiated one yet.
export const SCTP_DEFAULT_MESSAGE_SIZE = 65536;

export const DEFAULT_RTC_CONFIG: RTCConfiguration = {
  iceServers: [{ urls: "stun:stun.l.google.com:19302" }],
};
