import React, {useEffect, useState} from 'react';
import {Card, Divider, Progress, Table, Tag} from 'antd';
import {getDataNodeList, getNameNodeList} from "../../../services/service";
import {ProfileOutlined} from '@ant-design/icons';

function Dashboard() {
    const [datanodes, setDatanodes] = useState([]);
    const [namenodes, setNameNodes] = useState([]);
    const datNodeColumns = [
        {
            title: "节点ID",
            dataIndex: 'nodeId'
        },{

            title: "主机名",
            render: (text, record) => {
                return (<Tag icon={<ProfileOutlined />}style={{padding: '5px 10px'}} color="#55acee">{record.hostname}</Tag>)
            }
        }, {
            title: "状态",
            dataIndex: "status"
        }, {
            title: "存储空间",
            render: (text, record) => {
                return (
                    <div>
                        <div>
                            <Progress width={60} strokeLinecap="square" type="circle" percent={record.usePercent}/>
                        </div>
                        <div style={{marginTop: '5px'}}>
                            {record.storedDataSize} / {record.freeSpace}
                        </div>
                    </div>
                )
            }
        }, {
            title: '上次心跳时间',
            dataIndex: 'latestHeartbeatTime'
        }, {
            title: "HTTP端口",
            dataIndex: 'httpPort'
        }, {
            title: "NIO端口",
            dataIndex: 'nioPort'
        }
    ];

    const nameNodeColumns = [
        {
            title: "节点ID",
            dataIndex: "nodeId",
        },
        {
            title: "主机名",
            dataIndex: "hostname"
        },
        {
            title: "HTTP端口",
            dataIndex: "httpPort"
        },
        {
            title: "NIO端口",
            dataIndex: "nioPort"
        },
        {
            title: "Backup信息",
            dataIndex: "backupNodeInfo"
        }
    ];

    useEffect(() => {
            getDataNodeList()
                .then(res => {
                    setDatanodes(res.data)
                })
            getNameNodeList()
                .then(res => {
                    setNameNodes(res.data)
                })
        }
        , []);


    return (
        <Card title="节点列表">
            <Divider orientation="left">NameNode节点列表</Divider>
            <Table
                borderd
                rowKey="hostname"
                columns={nameNodeColumns}
                pagination={false}
                dataSource={namenodes}/>

            <Divider style={{marginTop: '100px'}} orientation="left">DataNode节点列表</Divider>
            <Table
                borderd
                rowKey="hostname"
                columns={datNodeColumns}
                pagination={false}
                dataSource={datanodes}/>
        </Card>
    )
}

export default Dashboard;