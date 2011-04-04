HandlerSocket for Kohana 3.x
============================

#What is HandlerSocket?

HandlerSocket is a NoSQL plugin for MySQL. Why should you use it? Imagine having an interface to your MySQL database that is almost twice as fast as Memcached.

##Installation

1. [Install the HandlerSocket Plugin](https://github.com/ahiguti/HandlerSocket-Plugin-for-MySQL)
2. [Install the HandlerSocket PHP5 Extension](https://code.google.com/p/php-handlersocket/)
3. Make sure your MySQL Server is secure. This is important, HandlerSocket doesn't perform any authentication, so you need to secure your MySQL Server like you would an instance of Memcached. This means only accepting connections from trusted IPs, an ironclad firewall configratuon, etc.
4. Copy this module to your Kohana modules directory and add it to your bootstrap.

##Getting Started

HandlerSocket doesn't support SQL (in fact, that's one of the reasons why it is so fast), but it can perform basic CRUD operations. This module offers three components:

1. An ORM-like interface for HandlerSocket called `HS`. I say ORM-like because there is no actual mapping going on. HandlerSocket can't perform a `DESCRIBE` or any other SQL query. Instead HandlerSocket relies on the indexes you define in your tables. You'll need to tell HandlerSocket which index to use (typically your Primary Key) and how to query it. More on that below.
2. A cache driver for [Kohana's Cachemodule](https://github.com/kohana/cache).
3. A session driver for [Kohana's Session library](https://github.com/kohana/core/tree/3.1%2Fmaster/classes/kohana/session)

#Important

This module is still under heavy development and is not stable. If you'd like to help please fork it.
