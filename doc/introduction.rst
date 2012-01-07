Turbulence
==========

Turbulence is a meta-data agnostic, but also meta-data aware streaming
database. Currently everyone uses their own databases with data stored in
custom formats. This means that data mining tasks require the use of multiple
APIs and the third party doing the mining has to spend considerable effort
parsing all these formats.

Consider distributed/open social networks (ref. The Locker Project, buddycloud,
Diaspora) where the key utility is streaming real-time updates across servers.
Or consider a set of motes monitoring environmental data (from weather to
seismic activity) and relaying them to a base station. Other services will want
to consume these updates by category. For example:

1) A contact aggregator may want to scrape links for hCard info, vCard like
   files or just phone numbers or pictures of contacts. He then subscribes to
   all kinds of sources.

2) A site like Last.fm wants to track your music listening, which includes
   audio files played on the computer, from Spotify, watched on Youtube,
   watched on Facebook and seen in VLC. They subscribe to your **multimedia**
   schema and immediately get updates about song titles and artists because
   **multimedia** is a *super-schema* of **audio** and **video**.
