/**
 * @author dpp
 * @date 2024/11/6
 * @Description
 *
 * NameServer 模块在整个消息队列系统中扮演着“服务发现”和“路由管理”角色。它的主要作用有以下几个方面：
 *
 * ##Broker 路由信息管理：
 *
 * NameServer 负责维护所有 Broker 的路由信息，记录每个 Broker 的地址、主题（Topic）信息等。
 * Broker 启动后会主动将自身的信息（IP 地址、端口、所属集群等）注册到 NameServer 上，并定期发送心跳包，保持连接。
 * 当 Broker 宕机或离线时，NameServer 会检测到并及时删除相关的路由信息。
 *
 * ##Topic 路由查找：
 *
 * Producer 和 Consumer 发送消息前需要获取消息所属 Topic 的路由信息（例如，哪些 Broker 存在该 Topic 的队列）。
 * Producer 和 Consumer 会向 NameServer 请求最新的 Topic 路由信息，从而知道消息该发往或从哪里消费。
 * 通过 NameServer 提供的路由信息，Producer 和 Consumer 能够实现负载均衡，找到最优的 Broker 节点。
 *
 * ##负载均衡：
 *
 * NameServer 中的路由信息帮助 Producer 和 Consumer 进行负载均衡。
 * 当多个 Broker 提供同一个 Topic 的消息服务时，客户端可以通过获取的路由信息选择合适的 Broker，避免请求集中到某一个 Broker 上，保证整个消息服务的高效分发。
 *
 * ##故障感知：
 *
 * NameServer 通过 Broker 发送的心跳包来感知 Broker 的状态。
 * 如果某个 Broker 在指定时间内没有发送心跳包，NameServer 会认为它已离线，将其从路由表中剔除，并将变更后的路由信息通知到客户端。
 *
 * ##多 NameServer 节点的高可用支持：
 *
 * NameServer 是一个无状态节点，通常部署为集群，支持横向扩展。
 * Producer 和 Consumer 可以连接到多个 NameServer 节点，当某个 NameServer 节点不可用时，客户端会自动切换到其他可用节点，保证服务的高可用性。
 *
 * ##提供轻量级的路由服务：
 *
 * NameServer 不直接存储和转发消息，而是提供一个轻量级的路由注册和查询服务。
 * NameServer 的设计非常轻量，能够快速响应客户端的路由查询请求，适合大规模部署。
 */
package com.dpp.minimq.namesrv;