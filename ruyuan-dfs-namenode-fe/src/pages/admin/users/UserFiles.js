import React, {useEffect, useState} from 'react';
import {Alert, Breadcrumb, Button, Card, InputNumber, message, Modal, Space, Table, Tag, Tooltip} from 'antd';
import {
    changeReplicaNum,
    getDataNodeByFilename,
    listUserFile,
    moveToTrash,
    recoverFromTrash
} from "../../../services/service";
import {CopyOutlined, DeleteOutlined, DownloadOutlined, EditTwoTone, FileOutlined} from '@ant-design/icons';
import {CopyToClipboard} from 'react-copy-to-clipboard'

function UserFiles(props) {

    const username = props.match.params.username

    const [path, setPath] = useState('/')
    const [children, setChildren] = useState([])
    const [isModalVisible, setIsModalVisible] = useState(false);
    const [isReplicaNumModalVisible, setIsReplicaNumModalVisible] = useState(false);
    const [beRemoveFile, setBeRemoveFile] = useState([]);
    const [dataNodes, setDataNodes] = useState({})
    const [loadStorage, setLoadStorage] = useState({})
    const [selectedRowKeys, setSelectedRowKeys] = useState([]);
    const [replicaNum, setReplicaNum] = useState(1);
    const [currentFilename, setCurrentFilename] = useState('');
    const basePath = '/'
    useEffect(() => {
        refresh(basePath)
    }, []);

    function isTrash() {
        if (props.location.state) {
            return props.location.state.trash;
        } else {
            const {search} = props.location;
            if (!search) {
                return false
            } else {
                const arr = search.split('&')
                const trash = arr[0].substr(7)
                return trash === 'true';
            }
        }
    }

    function refresh(path) {
        const param = {
            'username': username,
            'path': isTrash() ? '/.Trash' + path : path
        }
        listUserFile(param)
            .then(res => {
                if (res.data) {
                    setChildren(res.data.children)
                } else {
                    setChildren([])
                }
                setPath(path)
                setBeRemoveFile([])
                setSelectedRowKeys([])
                setDataNodes([])
            })
    }

    const handleDeleteOk = () => {
        const param = {
            'username': username,
            'paths': beRemoveFile
        }
        if (isTrash()) {
            console.log("param=", param)
            recoverFromTrash(param)
                .then(res => {
                    if (res.code === 0) {
                        setIsModalVisible(false);
                        message.success("?????????????????????????????????" + res.data, 5)
                        if (children.length > 1) {
                            refresh(path)
                        } else {
                            refresh(basePath)
                        }
                    } else {
                        message.error("???????????????" + res.msg)
                    }
                })
        } else {
            moveToTrash(param)
                .then(res => {
                    if (res.code === 0) {
                        setIsModalVisible(false);
                        message.success("?????????????????????????????????" + res.data, 5)
                        if (children.length > 1) {
                            refresh(path)
                        } else {
                            refresh(basePath)
                        }
                    } else {
                        message.error("???????????????" + res.msg)
                    }
                })
        }
    };

    const handleCancel = () => {
        setIsModalVisible(false);
        setIsReplicaNumModalVisible(false)
    };

    // ???????????????
    const clickBreadcrumb = (index) => {
        return () => {
            let pathSplit = path.split('/')
                .filter(item => item.length > 0)
            pathSplit = pathSplit.slice(0, index + 1);
            let newPath = '/' + pathSplit.join('/')
            refresh(newPath)
        }
    }

    // ??????????????????
    const handleDelete = (item) => {
        return () => {
            let delPath = concatPath(path, item.path)
            setBeRemoveFile([delPath])
            setIsModalVisible(true);
        }
    }

    // ????????????????????????
    const handleBatchDelete = () => {
        if (beRemoveFile.length === 0) {
            message.info("???????????????")
            return
        }
        setIsModalVisible(true);
    }


    const handleFetchStorageInfo = (item) => {
        return () => {
            let filename = path + '/' + item.path;
            const param = {
                'username': username,
                'path': filename
            }
            setLoadStorage((state) => {
                return {
                    ...state,
                    [item.path]: true
                }
            })
            getDataNodeByFilename(param)
                .then(res => {
                    console.log("datanodes=", res.data)
                    setLoadStorage((state) => {
                        return {
                            ...state,
                            [item.path]: false
                        }
                    })
                    setDataNodes((state) => {
                        return {
                            ...state,
                            [concatPath(path, item.path)]: res.data
                        }
                    })
                })
        }
    }

    function concatPath(p1, p2) {
        return p1 === '/' ? (p1 + p2) : (p1 + '/' + p2);
    }

    const rowSelection = {
        selectedRowKeys,
        onChange: (selectedRowKeys) => {
            setSelectedRowKeys(selectedRowKeys)
            const selectFullPath = selectedRowKeys.map(item => {
                return concatPath(path, item)
            });
            setBeRemoveFile(selectFullPath)
        },
    };


    const columns = [
        {
            title: '??????',
            render: (text, record, index) => {
                return index + 1;
            }
        }, {
            title: '?????????',
            render: (text, record) => {
                return <Space>
                    {record.type === 2 ?
                        <Button type="link" onClick={() => {
                            let newPath = concatPath(path, record.path)
                            refresh(newPath)
                        }}>{record.path}</Button> :
                        <div>{record.path}</div>}
                </Space>
            },
        }, {
            title: '????????????',
            render: (text, record) => {
                return record.type === 2 ? '--' : record.fileSize
            }
        },
        {
            title: '??????',
            render: (text, record) => {
                if (isTrash()) {
                    return <div>--</div>
                }
                if (record.type === 2) {
                    return <div>
                        <CopyToClipboard text={concatPath(path, record.path)} onCopy={() => {
                            message.success('???????????????');
                        }}>
                            <Button size={"small"} type={"default"} icon={<CopyOutlined/>}/>
                        </CopyToClipboard>
                    </div>
                } else {
                    return <Space size={"large"}>
                        <Button href={'http://localhost:8081/' + username + path + '/' + record.path}
                                target="_blank"
                                type="primary" shape="circle"
                                icon={<DownloadOutlined/>}
                        />
                        <CopyToClipboard text={concatPath(path, record.path)} onCopy={() => {
                            message.success('???????????????');
                        }}>
                            <Button type={"default"} icon={<CopyOutlined/>}/>
                        </CopyToClipboard>
                        <Button danger icon={<DeleteOutlined/>} onClick={handleDelete(record)}/>

                        <Tooltip title="??????????????????">
                            <Button icon={<EditTwoTone/>}
                                    onClick={() => {
                                        setIsReplicaNumModalVisible(true)
                                        setCurrentFilename(concatPath(path, record.path))
                                    }}
                            /></Tooltip>
                    </Space>
                }
            }
        },
        {
            title: '????????????',
            render: (text, record) => {
                if (record.type !== 1) {
                    return '--'
                }
                if (!dataNodes[concatPath(path, record.path)]) {
                    return (<Button
                            type="primary"
                            loading={loadStorage[record.path]}
                            icon={<FileOutlined/>}
                            onClick={handleFetchStorageInfo(record)
                            }>??????
                        </Button>
                    )
                }
                if (dataNodes[concatPath(path, record.path)].length === 0) {
                    return (<div/>)
                } else {
                    return dataNodes[concatPath(path, record.path)].map(node => {
                        return (
                            (<Space key={node} style={{
                                marginTop: "5px",
                            }}
                            >
                                <Tag color={"#2db7f5"}>{node.hostname}</Tag>
                            </Space>)
                        )
                    })
                }
            }
        }
    ]

    const onChaneReplicaNumOk = () => {
        const param = {
            'path': currentFilename,
            'username': username,
            'replicaNum': replicaNum
        }
        console.log("param =", param)
        changeReplicaNum(param)
            .then(res => {
                if (res.code === 0) {
                    message.success('???????????????????????????????????????????????????', 5)
                } else {
                    message.error("???????????????" + res.msg, 5)
                }
                setIsReplicaNumModalVisible(false)
            })

    }

    return (<Card title={<Space>
            <Button type={"primary"} onClick={() => {
                props.history.push("/admin/users")
            }
            }>??????</Button>
            <div>???{username}??????{isTrash() ? '?????????' : '????????????'}</div>
        </Space>} extra={
            <Button type="primary" size="default" danger={!isTrash()}
                    onClick={handleBatchDelete}>{isTrash() ? '??????' : '??????'}????????????/?????????</Button>
        }>
            <Modal
                title={isTrash() ? '????????????' : '????????????'} visible={isModalVisible} onOk={handleDeleteOk}
                onCancel={handleCancel}>
                <Alert
                    message={isTrash() ? '????????????????????????????????????????????????????????????????????????' : '???????????????????????????????????????????????????????????????????????????'}
                    type={isTrash() ? 'success' : 'warning'}
                />
                <p style={{marginTop: '10px'}}>???{isTrash() ? '??????' : '??????'}????????????:</p>
                {beRemoveFile.map(item => {
                    return <Tag style={{marginTop: '10px'}} key={item} color="magenta">{item}</Tag>
                })}
            </Modal>

            <Modal
                title="??????????????????" visible={isReplicaNumModalVisible} onOk={onChaneReplicaNumOk}
                onCancel={handleCancel}>
                ???????????????<InputNumber min={1} max={5} defaultValue={1} onChange={(value) => {
                setReplicaNum(value)
            }}/>
            </Modal>

            <Table
                title={() => {
                    return <Space>
                        <Breadcrumb>
                            <Breadcrumb.Item>
                                <Button type="link" onClick={() => {
                                    refresh(basePath)
                                }} size="large" style={{
                                    padding: '0 0'
                                }}>Home</Button>
                            </Breadcrumb.Item>
                            {
                                path.split('/')
                                    .filter(item => item.length > 0)
                                    .map((e, index) => {
                                        return (
                                            <Breadcrumb.Item key={index}>
                                                <Button size="large" type="link" style={{
                                                    padding: '0 0'
                                                }} onClick={clickBreadcrumb(index)}
                                                >{e}</Button>
                                            </Breadcrumb.Item>
                                        )
                                    })
                            }
                        </Breadcrumb>
                        <CopyToClipboard text={path} onCopy={() => {
                            message.success('???????????????');
                        }}>
                            <Button size={"small"} type={"default"} icon={<CopyOutlined/>}/>
                        </CopyToClipboard>
                    </Space>
                }}
                borderd
                rowKey="path"
                expandable={{
                    expandedRowRender: record => <div/>,
                    rowExpandable: record => false
                }}
                rowSelection={rowSelection}
                columns={columns}
                dataSource={children}>
            </Table>
        </Card>
    )
}

export default UserFiles;