import React, {useEffect, useState} from 'react';
import {Button, Space, Card, message, Popconfirm, Table, Tag, Tooltip} from 'antd';
import {deleteUser, getUserList} from "../../../services/service";
import {QuestionCircleTwoTone, DeleteTwoTone, EditTwoTone, DeleteFilled} from "@ant-design/icons";


function UserList(props) {

    const [userList, setUserList] = useState([]);

    const columns = [
        {
            title: "用户名",
            render: (text, record) => {
                return (
                    <Button onClick={() => {
                        props.history.push({
                            pathname: "/admin/users/files/" + record.username,
                            state: {trash: false}
                        })
                    }} type="link" size="default">
                        {record.username}
                    </Button>
                )
            }
        },
        {
            title: "用户秘钥",
            dataIndex: "secret"
        },
        {
            title: "文件数量",
            render: (text, record) => {
                return (
                    <div key={record.storageInfo.fileCount} style={{
                        marginTop: "5px",
                    }}><Tag color="#108ee9">{record.storageInfo.fileCount}</Tag></div>
                )
            }
        },
        {
            title: () => {
                return (
                    <div>
            <span style={{
                marginRight: "10px"
            }}>文件大小</span>
                        <Tooltip title="该文件大小不包括副本文件占用的内存空间"><QuestionCircleTwoTone/></Tooltip>
                    </div>
                )
            },
            render: (text, record) => {
                return (<Tag>{record.storageInfo.displayStorageSize}</Tag>)
            }
        },
        {
            title: "DataNode配额",
            render: (text, record) => {
                const nodes = record.storageInfo.dataNodes;
                if (nodes.length === 0) {
                    return (<div>无配额</div>)
                } else {
                    return nodes.map(node => {
                        return (
                            (<div key={node} style={{
                                marginTop: "5px",
                            }}
                            ><Tag color={"#2db7f5"}>{node}</Tag>
                            </div>)
                        )
                    })
                }
            }
        },
        {
            title: "操作",
            "render": (text, record) => {
                return (
                    <Space size={"large"}>
                        <Tooltip title="修改">
                            <Button icon={<EditTwoTone/>}
                                    size={"large"}
                                    onClick={() => {
                                        props.history.push("/admin/users/add/" + record.username);
                                    }}
                            /></Tooltip>
                        <Popconfirm
                            title="确定删除此项？"
                            okText={"确认"}
                            cancelText={"取消"}
                            onCancel={() => {
                                console.log("用户取消删除")
                            }}
                            onConfirm={() => {
                                console.log("用户确认删除", record)
                                deleteUser(record.username)
                                    .then(res => {
                                        if (res.code === 0) {
                                            message.success("删除成功")
                                            refreshUserList()
                                        } else {
                                            message.success("删除失败")
                                        }
                                    }).catch(err => {
                                    message.success("删除失败:" + err)
                                })
                            }}
                        >
                            <Tooltip title="删除">
                                <Button size="large" icon={<DeleteTwoTone/>}/>
                            </Tooltip>
                        </Popconfirm>
                        <Button icon={<DeleteFilled/>} size="large"
                                onClick={showUserTrash(record.username)}>查看垃圾桶</Button>
                    </Space>
                )
            }
        }
    ];

    const showUserTrash = (username) => {
        return () => {
            props.history.push({
                pathname: "/admin/users/files/" + username,
                search: "?trash=true",
                state: {trash: true}
            })
        }
    }

    useEffect(() => {
        refreshUserList()
    }, []);

    function refreshUserList() {
        getUserList()
            .then(res => {
                setUserList(res.data)
            })
    }

    return (
        <div>
            <Card title="用户列表" extra={
                <Button type="primary" size="default" onClick={() => {
                    props.history.push("/admin/users/add/")
                }}>新增用户</Button>
            }>
                <Table
                    borderd
                    rowKey="username"
                    columns={columns}
                    dataSource={userList}/>
            </Card>
        </div>
    )
}

export default UserList;